"""
Predictive model for Madrid real estate price estimation.

Improvements over original:
- Real confidence intervals from individual tree predictions (not heuristic ±10%)
- Cross-validation with metrics: R², MAE, MAPE, RMSE
- Model metadata: training date, sample size, feature importances
- Periodic retraining mechanism (configurable staleness threshold)
- Quantile-based prediction intervals (10th-90th percentile across trees)
"""

import os
import json
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_validate, train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)

import streamlit as st
from database import get_connection
from coordinates import distance_to_sol


# ============================================================================
# CONFIGURATION
# ============================================================================

# Retraining: model is considered stale after this many days
MODEL_STALENESS_DAYS = int(os.getenv("MODEL_STALENESS_DAYS", "7"))

# File to persist model metadata (metrics, training date, etc.)
MODEL_META_FILE = "model_metadata.json"

# Number of cross-validation folds
CV_FOLDS = 5

# Random Forest hyperparameters
RF_N_ESTIMATORS = 200
RF_MAX_DEPTH = None  # unlimited
RF_MIN_SAMPLES_LEAF = 5
RF_RANDOM_STATE = 42


# ============================================================================
# MODEL CLASS
# ============================================================================

class PricePredictor:
    def __init__(self):
        self.model: Optional[Pipeline] = None
        self.is_trained: bool = False
        self.feature_columns = [
            'distrito', 'barrio', 'size_sqm', 'rooms',
            'floor_level', 'has_lift', 'is_exterior',
            # Barrio-context features (added June 2026, ROADMAP §2.3):
            'barrio_median_sqm', 'barrio_dist_sol', 'barrio_supply', 'barrio_velocity',
        ]
        self.metrics: Dict = {}
        self.training_date: Optional[str] = None
        self.training_samples: int = 0
        self.feature_importances: Dict[str, float] = {}

        # Barrio-level context learned at train time, keyed by "distrito||barrio".
        # Persisted so predict() can enrich a single listing without re-querying
        # the whole table. ``_barrio_default`` is the city-wide fallback for
        # barrios unseen at training time.
        self.barrio_context: Dict[str, Dict[str, float]] = {}
        self._barrio_default: Dict[str, float] = {}

        # Try to load saved metadata
        self._load_metadata()

    # ------------------------------------------------------------------
    # DATA LOADING & PREPROCESSING
    # ------------------------------------------------------------------

    def load_training_data(self) -> pd.DataFrame:
        """Load and preprocess data from database."""
        with get_connection() as conn:
            query = """
            SELECT price, distrito, barrio, size_sqm, rooms,
                   floor, orientation, seller_type,
                   status, first_seen_date, last_seen_date
            FROM listings
            WHERE status IN ('active', 'sold_removed')
              AND price IS NOT NULL
              AND size_sqm IS NOT NULL
              AND size_sqm > 10
              AND price > 10000
            """
            df = pd.read_sql_query(query, conn)

        return self._preprocess_data(df)

    def _parse_floor(self, floor_str) -> Tuple[float, int]:
        """Parse floor string to extract level and lift info."""
        if pd.isna(floor_str):
            return 0.0, 0

        floor_str = str(floor_str).lower()
        level = 0.0
        has_lift = 0

        # Lift detection
        if 'con ascensor' in floor_str:
            has_lift = 1
        elif 'sin ascensor' in floor_str:
            has_lift = 0

        # Level detection
        if 'bajo' in floor_str or 'sótano' in floor_str or 'semi-sótano' in floor_str:
            level = 0.0
        elif 'entreplanta' in floor_str:
            level = 0.5
        elif 'planta' in floor_str:
            match = re.search(r'planta (\d+)', floor_str)
            if match:
                level = float(match.group(1))
        elif 'ático' in floor_str:
            level = 10.0

        return level, has_lift

    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Feature engineering."""
        parsed_floors = df['floor'].apply(self._parse_floor)
        df['floor_level'] = parsed_floors.apply(lambda x: x[0])
        df['has_lift'] = parsed_floors.apply(lambda x: x[1])
        df['is_exterior'] = df['orientation'].apply(
            lambda x: 1 if str(x).lower() == 'exterior' else 0
        )
        df['rooms'] = df['rooms'].fillna(1)
        df['price_sqm'] = df['price'] / df['size_sqm']

        # Days on market (only meaningful for sold listings → barrio velocity).
        if 'first_seen_date' in df and 'last_seen_date' in df:
            first = pd.to_datetime(df['first_seen_date'], errors='coerce')
            last = pd.to_datetime(df['last_seen_date'], errors='coerce')
            df['days_on_market'] = (last - first).dt.days
        else:
            df['days_on_market'] = np.nan

        # Remove extreme outliers (top/bottom 1%)
        q_low = df['price_sqm'].quantile(0.01)
        q_high = df['price_sqm'].quantile(0.99)
        df_clean = df[(df['price_sqm'] > q_low) & (df['price_sqm'] < q_high)].copy()

        # Learn barrio context from the cleaned set, then attach the 4 context
        # features to every row (and remember it for predict()).
        self.barrio_context, self._barrio_default = self._compute_barrio_context(df_clean)
        self._attach_barrio_features(df_clean)

        return df_clean

    # ------------------------------------------------------------------
    # BARRIO-CONTEXT FEATURES
    # ------------------------------------------------------------------

    @staticmethod
    def _barrio_key(distrito, barrio) -> str:
        return f"{distrito}||{barrio}"

    def _compute_barrio_context(
        self, df: pd.DataFrame
    ) -> Tuple[Dict[str, Dict[str, float]], Dict[str, float]]:
        """
        Aggregate, per (distrito, barrio):
            median_sqm  median €/m² of the barrio (price level)
            supply      number of *active* listings (offer density)
            velocity    median days-on-market of *sold* listings (how fast it moves)

        Returns (context_by_key, city_default). The median is a barrio-level
        aggregate, so a single listing barely moves it — but note the value is
        computed over the full dataset (test rows included), a small optimistic
        bias in CV that a fully leak-proof pipeline would fold inside each fold.
        """
        context: Dict[str, Dict[str, float]] = {}
        for (distrito, barrio), g in df.groupby(['distrito', 'barrio'], dropna=False):
            sold_days = g.loc[g['status'] == 'sold_removed', 'days_on_market'].dropna()
            context[self._barrio_key(distrito, barrio)] = {
                'median_sqm': float(g['price_sqm'].median()),
                'supply': int((g['status'] == 'active').sum()),
                'velocity': float(sold_days.median()) if len(sold_days) else None,
            }

        all_sold_days = df.loc[df['status'] == 'sold_removed', 'days_on_market'].dropna()
        default = {
            'median_sqm': float(df['price_sqm'].median()),
            'supply': int(np.median([c['supply'] for c in context.values()])) if context else 0,
            'velocity': float(all_sold_days.median()) if len(all_sold_days) else None,
        }
        return context, default

    def _lookup_barrio(self, distrito, barrio) -> Dict[str, float]:
        """Resolve the 4 context features for one listing, falling back to the
        city-wide defaults for unseen barrios / missing velocity."""
        ctx = self.barrio_context.get(self._barrio_key(distrito, barrio), {})
        default = self._barrio_default or {}
        velocity = ctx.get('velocity')
        if velocity is None:
            velocity = default.get('velocity')
        return {
            'barrio_median_sqm': ctx.get('median_sqm', default.get('median_sqm')),
            'barrio_dist_sol': distance_to_sol(distrito, barrio),  # None → imputed
            'barrio_supply': ctx.get('supply', default.get('supply')),
            'barrio_velocity': velocity,
        }

    def _attach_barrio_features(self, df: pd.DataFrame) -> None:
        """Vectorised attach of the 4 context features onto a training frame."""
        rows = [
            self._lookup_barrio(d, b)
            for d, b in zip(df['distrito'], df['barrio'])
        ]
        for col in ('barrio_median_sqm', 'barrio_dist_sol', 'barrio_supply', 'barrio_velocity'):
            df[col] = [r[col] for r in rows]

    # ------------------------------------------------------------------
    # TRAINING WITH CROSS-VALIDATION
    # ------------------------------------------------------------------

    def _build_pipeline(self) -> Pipeline:
        """Build the sklearn pipeline (preprocessor + regressor)."""
        numeric_features = [
            'size_sqm', 'rooms', 'floor_level', 'has_lift', 'is_exterior',
            'barrio_median_sqm', 'barrio_dist_sol', 'barrio_supply', 'barrio_velocity',
        ]
        categorical_features = ['distrito', 'barrio']

        numeric_transformer = SimpleImputer(strategy='median')
        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('onehot', OneHotEncoder(handle_unknown='ignore')),
        ])

        preprocessor = ColumnTransformer(transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features),
        ])

        pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('regressor', RandomForestRegressor(
                n_estimators=RF_N_ESTIMATORS,
                max_depth=RF_MAX_DEPTH,
                min_samples_leaf=RF_MIN_SAMPLES_LEAF,
                random_state=RF_RANDOM_STATE,
                n_jobs=-1,
            )),
        ])
        return pipeline

    def train(self) -> Tuple[bool, str]:
        """
        Train the model with cross-validation.

        Returns:
            (success, message) tuple
        """
        df = self.load_training_data()

        if len(df) < 50:
            return False, "Insuficientes datos para entrenar el modelo (mínimo 50 propiedades)"

        X = df[self.feature_columns]
        y = df['price']

        # ----- Cross-validation metrics -----
        pipeline = self._build_pipeline()

        scoring = {
            'r2': 'r2',
            'mae': 'neg_mean_absolute_error',
            'rmse': 'neg_root_mean_squared_error',
            'mape': 'neg_mean_absolute_percentage_error',
        }

        cv_results = cross_validate(
            pipeline, X, y,
            cv=CV_FOLDS,
            scoring=scoring,
            return_train_score=False,
        )

        self.metrics = {
            'r2': float(np.mean(cv_results['test_r2'])),
            'r2_std': float(np.std(cv_results['test_r2'])),
            'mae': float(-np.mean(cv_results['test_mae'])),
            'mae_std': float(np.std(cv_results['test_mae'])),
            'rmse': float(-np.mean(cv_results['test_rmse'])),
            'rmse_std': float(np.std(cv_results['test_rmse'])),
            'mape': float(-np.mean(cv_results['test_mape']) * 100),
            'mape_std': float(np.std(cv_results['test_mape']) * 100),
        }

        # ----- Final model: train on ALL data -----
        self.model = self._build_pipeline()
        self.model.fit(X, y)

        # ----- Feature importances -----
        self._compute_feature_importances(X)

        # ----- Metadata -----
        self.is_trained = True
        self.training_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.training_samples = len(df)

        self._save_metadata()

        return True, (
            f"Modelo entrenado con {len(df)} propiedades "
            f"(R²={self.metrics['r2']:.3f}, MAE=€{self.metrics['mae']:,.0f}, "
            f"MAPE={self.metrics['mape']:.1f}%)"
        )

    def _compute_feature_importances(self, X: pd.DataFrame) -> None:
        """Extract and store feature importances from the trained model."""
        regressor = self.model.named_steps['regressor']
        preprocessor = self.model.named_steps['preprocessor']

        # Get transformed feature names
        try:
            feature_names = preprocessor.get_feature_names_out()
        except AttributeError:
            feature_names = [f"feature_{i}" for i in range(len(regressor.feature_importances_))]

        importances = regressor.feature_importances_

        # Aggregate one-hot categories back to original feature name
        agg = {}
        for name, imp in zip(feature_names, importances):
            # Names look like 'num__size_sqm' or 'cat__barrio_Acacias'
            base = name.split('__')[-1].split('_')[0] if '__' in name else name
            # Map back to recognisable group. Order matters: the barrio-context
            # numeric features contain the substring 'barrio', so they must be
            # matched BEFORE the categorical one-hot 'barrio' catch-all.
            if 'barrio_median_sqm' in name:
                base = 'barrio_median_sqm'
            elif 'barrio_dist_sol' in name:
                base = 'barrio_dist_sol'
            elif 'barrio_supply' in name:
                base = 'barrio_supply'
            elif 'barrio_velocity' in name:
                base = 'barrio_velocity'
            elif 'size' in name:
                base = 'size_sqm'
            elif 'rooms' in name:
                base = 'rooms'
            elif 'floor' in name:
                base = 'floor_level'
            elif 'lift' in name:
                base = 'has_lift'
            elif 'exterior' in name:
                base = 'is_exterior'
            elif 'distrito' in name:
                base = 'distrito'
            elif 'barrio' in name:
                base = 'barrio'
            else:
                base = name

            agg[base] = agg.get(base, 0.0) + float(imp)

        # Sort descending
        self.feature_importances = dict(
            sorted(agg.items(), key=lambda x: x[1], reverse=True)
        )

    # ------------------------------------------------------------------
    # PREDICTION WITH REAL CONFIDENCE INTERVALS
    # ------------------------------------------------------------------

    def predict(self, features: Dict) -> Dict:
        """
        Predict price with real confidence intervals derived from
        individual tree predictions (quantile approach).

        Returns dict with:
            estimated_price, lower_bound, upper_bound, std_dev,
            confidence_pct (actual spread as %)
        """
        if not self.is_trained or self._is_stale():
            self.train()

        # Enrich the listing with the learned barrio-context features so the
        # caller only has to pass the raw attributes (distrito/barrio/size/…).
        enriched = dict(features)
        enriched.update(self._lookup_barrio(features.get('distrito'), features.get('barrio')))
        input_df = pd.DataFrame([enriched])

        # Central prediction
        prediction = self.model.predict(input_df)[0]

        # --- Real confidence interval from individual trees ---
        regressor = self.model.named_steps['regressor']
        preprocessor = self.model.named_steps['preprocessor']

        X_transformed = preprocessor.transform(input_df)

        # Collect predictions from each tree
        tree_predictions = np.array([
            tree.predict(X_transformed)[0]
            for tree in regressor.estimators_
        ])

        # Quantile-based interval (10th - 90th percentile)
        lower_bound = float(np.percentile(tree_predictions, 10))
        upper_bound = float(np.percentile(tree_predictions, 90))
        std_dev = float(np.std(tree_predictions))

        # Confidence spread as % of central prediction
        spread_pct = ((upper_bound - lower_bound) / prediction * 100) if prediction > 0 else 0

        return {
            'estimated_price': float(prediction),
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'std_dev': std_dev,
            'confidence_pct': round(spread_pct, 1),
        }

    # ------------------------------------------------------------------
    # PERIODIC RETRAINING
    # ------------------------------------------------------------------

    def _is_stale(self) -> bool:
        """Check if the model needs retraining."""
        if not self.training_date:
            return True

        try:
            trained_at = datetime.strptime(self.training_date, "%Y-%m-%d %H:%M:%S")
            return (datetime.now() - trained_at) > timedelta(days=MODEL_STALENESS_DAYS)
        except (ValueError, TypeError):
            return True

    def needs_retraining(self) -> Tuple[bool, str]:
        """
        Public check — returns (needs_retrain, reason).
        Useful for the dashboard to show a warning.
        """
        if not self.is_trained:
            return True, "El modelo no ha sido entrenado todavía"
        if self._is_stale():
            days = MODEL_STALENESS_DAYS
            return True, f"El modelo tiene más de {days} días sin reentrenar"
        return False, ""

    # ------------------------------------------------------------------
    # METADATA PERSISTENCE
    # ------------------------------------------------------------------

    def _save_metadata(self) -> None:
        """Persist model metrics and training info to JSON."""
        meta = {
            'training_date': self.training_date,
            'training_samples': self.training_samples,
            'metrics': self.metrics,
            'feature_importances': self.feature_importances,
            'barrio_context': self.barrio_context,
            'barrio_default': self._barrio_default,
            'config': {
                'n_estimators': RF_N_ESTIMATORS,
                'max_depth': RF_MAX_DEPTH,
                'min_samples_leaf': RF_MIN_SAMPLES_LEAF,
                'cv_folds': CV_FOLDS,
                'staleness_days': MODEL_STALENESS_DAYS,
            },
        }
        try:
            with open(MODEL_META_FILE, 'w') as f:
                json.dump(meta, f, indent=2, ensure_ascii=False)
        except IOError:
            pass

    def _load_metadata(self) -> None:
        """Load previously saved metadata."""
        try:
            if Path(MODEL_META_FILE).exists():
                with open(MODEL_META_FILE, 'r') as f:
                    meta = json.load(f)
                self.training_date = meta.get('training_date')
                self.training_samples = meta.get('training_samples', 0)
                self.metrics = meta.get('metrics', {})
                self.feature_importances = meta.get('feature_importances', {})
                self.barrio_context = meta.get('barrio_context', {})
                self._barrio_default = meta.get('barrio_default', {})
        except (json.JSONDecodeError, IOError):
            pass

    def get_model_info(self) -> Dict:
        """Return a summary dict for displaying in the dashboard."""
        stale, reason = self.needs_retraining()
        return {
            'is_trained': self.is_trained,
            'training_date': self.training_date,
            'training_samples': self.training_samples,
            'metrics': self.metrics,
            'feature_importances': self.feature_importances,
            'needs_retraining': stale,
            'retraining_reason': reason,
            'config': {
                'n_estimators': RF_N_ESTIMATORS,
                'cv_folds': CV_FOLDS,
                'staleness_days': MODEL_STALENESS_DAYS,
            },
        }


# Singleton — avoids retraining on every Streamlit interaction
predictor = PricePredictor()
