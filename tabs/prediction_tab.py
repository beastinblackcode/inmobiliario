"""
Prediction tab — AI property valuation with confidence intervals.

Entry point: render_prediction_tab(df)
"""

import streamlit as st
import pandas as pd
import plotly.express as px


def render_prediction_tab(df: pd.DataFrame) -> None:
    """Render all content for the 🔮 Predicción tab."""

    from predictive_model import predictor

    st.markdown("---")
    st.subheader("🔮 Valuación de Propiedades con IA")

    # Model status bar
    model_info = predictor.get_model_info()
    if model_info["needs_retraining"]:
        st.warning(
            f"⚠️ {model_info['retraining_reason']}. Se reentrenará automáticamente al calcular."
        )
    elif model_info["is_trained"] and model_info["metrics"]:
        m = model_info["metrics"]
        st.success(
            f"✅ Modelo activo — Entrenado: {model_info['training_date'][:10]} · "
            f"{model_info['training_samples']:,} propiedades · "
            f"R²={m.get('r2', 0):.3f} · MAE=€{m.get('mae', 0):,.0f} · "
            f"MAPE={m.get('mape', 0):.1f}%"
        )

    col1, col2 = st.columns([1, 2])

    # -------------------------------------------------------------------------
    # Left column — inputs
    # -------------------------------------------------------------------------
    with col1:
        st.markdown("### 📝 Características")

        districts = sorted(
            [d for d in df["distrito"].unique().tolist() if d is not None]
        )
        if not districts:
            districts = ["Centro"]

        sel_col, _ = st.columns([1, 0.01])
        with sel_col:
            selected_district = st.selectbox("Distrito", options=districts)

        district_barrios = sorted(
            [
                b
                for b in df[df["distrito"] == selected_district]["barrio"]
                .unique()
                .tolist()
                if b is not None
            ]
        )
        if not district_barrios:
            district_barrios = ["General"]

        with st.form("prediction_form"):
            selected_barrio = st.selectbox("Barrio", options=district_barrios)

            col_a, col_b = st.columns(2)
            with col_a:
                size_input = st.number_input(
                    "Superficie (m²)", min_value=20, max_value=500, value=80, step=1
                )
            with col_b:
                rooms_input = st.number_input(
                    "Habitaciones", min_value=0, max_value=10, value=2, step=1
                )

            floor_options = {
                "Bajo / Sótano": 0,
                "Entreplanta": 0.5,
                "1ª Planta": 1,
                "2ª Planta": 2,
                "3ª Planta": 3,
                "4ª Planta": 4,
                "5ª Planta": 5,
                "6ª Planta": 6,
                "7ª+ Planta": 7,
                "Ático": 10,
            }
            selected_floor_label = st.selectbox(
                "Altura", options=list(floor_options.keys()), index=2
            )
            floor_level = floor_options[selected_floor_label]

            col_c, col_d = st.columns(2)
            with col_c:
                has_lift = st.checkbox("Ascensor", value=True)
            with col_d:
                is_exterior = st.checkbox("Exterior", value=True)

            submitted = st.form_submit_button(
                "💰 Calcular Precio", type="primary"
            )

        # Model details expander (below the form)
        with st.expander("📈 Rendimiento del Modelo"):
            if model_info["is_trained"] and model_info["metrics"]:
                m = model_info["metrics"]

                perf_c1, perf_c2, perf_c3, perf_c4 = st.columns(4)
                perf_c1.metric(
                    "R²",
                    f"{m.get('r2', 0):.3f}",
                    help="Coeficiente de determinación (1.0 = perfecto)",
                )
                perf_c2.metric(
                    "MAE",
                    f"€{m.get('mae', 0):,.0f}",
                    help="Error absoluto medio",
                )
                perf_c3.metric(
                    "RMSE",
                    f"€{m.get('rmse', 0):,.0f}",
                    help="Raíz del error cuadrático medio",
                )
                perf_c4.metric(
                    "MAPE",
                    f"{m.get('mape', 0):.1f}%",
                    help="Error porcentual absoluto medio",
                )

                st.caption(
                    f"Validación cruzada ({model_info['config']['cv_folds']} folds) · "
                    f"{model_info['config']['n_estimators']} árboles · "
                    f"Reentrenamiento cada {model_info['config']['staleness_days']} días"
                )

                # Feature importances chart
                if model_info["feature_importances"]:
                    fi = model_info["feature_importances"]
                    fi_labels = {
                        "size_sqm": "Superficie",
                        "barrio": "Barrio",
                        "distrito": "Distrito",
                        "rooms": "Habitaciones",
                        "floor_level": "Altura",
                        "has_lift": "Ascensor",
                        "is_exterior": "Exterior",
                    }
                    fi_df = pd.DataFrame(
                        {
                            "Variable": [
                                fi_labels.get(k, k) for k in fi.keys()
                            ],
                            "Importancia": [v * 100 for v in fi.values()],
                        }
                    )
                    st.bar_chart(fi_df.set_index("Variable"), height=200)
            else:
                st.info(
                    "El modelo aún no ha sido entrenado. Pulsa 'Calcular Precio' para entrenar."
                )

            # Manual retrain button
            if st.button("🔄 Forzar reentrenamiento"):
                with st.spinner("Reentrenando modelo..."):
                    ok, msg = predictor.train()
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

    # -------------------------------------------------------------------------
    # Right column — results
    # -------------------------------------------------------------------------
    with col2:
        st.markdown("### 📊 Resultado de la Valuación")

        if submitted:
            with st.spinner("Calculando precio estimado..."):
                features = {
                    "distrito": selected_district,
                    "barrio": selected_barrio,
                    "size_sqm": size_input,
                    "rooms": rooms_input,
                    "floor_level": floor_level,
                    "has_lift": 1 if has_lift else 0,
                    "is_exterior": 1 if is_exterior else 0,
                }

                try:
                    result = predictor.predict(features)

                    est_price = result["estimated_price"]
                    lower = result["lower_bound"]
                    upper = result["upper_bound"]
                    std_dev = result["std_dev"]
                    conf_pct = result["confidence_pct"]

                    # Calculate actual percentage deltas from central estimate
                    lower_pct = (
                        ((lower - est_price) / est_price * 100)
                        if est_price > 0
                        else 0
                    )
                    upper_pct = (
                        ((upper - est_price) / est_price * 100)
                        if est_price > 0
                        else 0
                    )

                    metric_col1, metric_col2, metric_col3 = st.columns(3)

                    with metric_col1:
                        st.metric("Precio Estimado", f"€{est_price:,.0f}")
                    with metric_col2:
                        st.metric(
                            "Rango Inferior (P10)",
                            f"€{lower:,.0f}",
                            delta=f"{lower_pct:+.1f}%",
                            delta_color="normal",
                        )
                    with metric_col3:
                        st.metric(
                            "Rango Superior (P90)",
                            f"€{upper:,.0f}",
                            delta=f"{upper_pct:+.1f}%",
                            delta_color="normal",
                        )

                    st.caption(
                        f"Intervalo de confianza real (percentil 10-90 de "
                        f"{predictor.model.named_steps['regressor'].n_estimators} árboles) · "
                        f"Dispersión: ±{conf_pct/2:.1f}% · σ = €{std_dev:,.0f}"
                    )

                    # Price per sqm analysis
                    est_sqm = est_price / size_input
                    avg_zone_sqm = df[
                        (df["distrito"] == selected_district)
                        & (df["barrio"] == selected_barrio)
                    ]["price_per_sqm"].mean()

                    st.markdown("---")
                    st.markdown(
                        f"**Análisis:** El precio estimado es de **€{est_sqm:,.0f}/m²**."
                    )

                    if pd.notna(avg_zone_sqm):
                        diff_pct = (
                            (est_sqm - avg_zone_sqm) / avg_zone_sqm
                        ) * 100
                        status_text = (
                            "por encima" if diff_pct > 0 else "por debajo"
                        )
                        color = "red" if diff_pct > 0 else "green"
                        st.markdown(
                            f"El promedio de la zona ({selected_barrio}) es **€{avg_zone_sqm:,.0f}/m²**. "
                            f"Esta propiedad está un "
                            f"<span style='color:{color}'>{abs(diff_pct):.1f}% {status_text}</span> "
                            f"del promedio.",
                            unsafe_allow_html=True,
                        )

                    # Distribution plot
                    st.markdown("#### Distribución de Precios en la Zona")
                    zone_df = df[
                        (df["distrito"] == selected_district)
                        & (df["size_sqm"] >= size_input * 0.7)
                        & (df["size_sqm"] <= size_input * 1.3)
                    ]

                    if len(zone_df) > 5:
                        fig = px.histogram(
                            zone_df,
                            x="price",
                            nbins=20,
                            title=f"Propiedades similares en {selected_district} (±30% tamaño)",
                            color_discrete_sequence=["lightgray"],
                        )
                        fig.add_vline(
                            x=est_price,
                            line_width=3,
                            line_dash="dash",
                            line_color="blue",
                            annotation_text="Estimación",
                        )
                        fig.add_vrect(
                            x0=lower,
                            x1=upper,
                            fillcolor="blue",
                            opacity=0.1,
                            line_width=0,
                            annotation_text="IC 80%",
                            annotation_position="top left",
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning(
                            "No hay suficientes propiedades similares en la zona "
                            "para generar un gráfico de distribución."
                        )

                except Exception as e:
                    st.error(f"Error en la predicción: {str(e)}")
        else:
            st.info(
                "👈 Configura las características de la propiedad y pulsa "
                "'Calcular Precio' para ver la estimación."
            )
            st.markdown("---")
            st.caption(
                "ℹ️ El modelo se entrena automáticamente y se reentrena "
                "periódicamente con datos actualizados."
            )
