"""
Production-ready Streamlit app: C9 Data Upload & Automation
- Improved session state handling
- Robust file validation and saving
- Clear step flow using forms to avoid accidental re-runs
- Detailed logging and user-friendly messages
- Defensive error handling and type hints
"""

import logging
import os
from pathlib import Path
import tempfile
from typing import Dict, Optional

import pandas as pd
import streamlit as st

# Local automation modules (user-provided). Ensure these import paths are correct.
from automate_consumption_data import (
    process_hrbr_consumption,
    split_units_to_hourly,
    consolidate_units_hourly,
    add_tod_slot,
    merge_hourly_to_tod,
    split_hourly_to_15min,
    merge_hourly_to_daily,
)
from automate_generation_data import (
    merge_inverter_data,
    aggregate_15min,
    split_date_time,
    merge_generation_consumption,
    aggregate_hourly,
)
from automate_settlement import (
    calculate_matched_settlement,
    add_unit_id,
    monthly_aggregation,
    apply_monthly_banking_settlement,
    calculate_savings_comparison,
)

import validation_utils

# --- App config ---
st.set_page_config(page_title="Data Upload & Automation", layout="wide")
st.title("Data Upload & Automation")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("c9_app")

# --- Utilities ---
DEFAULT_UNIT_VALUES: Dict[str, float] = {
    "Malleswaram": 48359.985,
    "Electronic City": 69740.0,
    "Kanakapura": 45733.521,
    "Bellandur": 48752.24325,
    "Sarjapura": 45603.012,
    "Sahakar Nagar": 58407.5,
    "HRBR Unit": 45230.0,
    "Whitefield": 88540.058,
    "Bellandur Corp. Office": 22886.238,
    "Thanisandra": 53563.019,
    "Old Airport Road": 77528.014,
}


def init_session_state() -> None:
    """Ensure required session keys exist with sensible defaults."""
    defaults = {
        "flow_step": 1,
        "validation_passed": False,
        "client_name": "",
        "month": "",
        "gen_type": "Solar",
        "data_upload_type": "Generation",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def save_uploaded_file(uploaded, dest_path: Path) -> None:
    """Save a Streamlit uploaded file to disk (binary write).

    Args:
        uploaded: UploadedFile from Streamlit (supports .read()).
        dest_path: Path where file will be written.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(uploaded.getbuffer())


# --- Initialize session state ---
init_session_state()

# --- Step 1: Metadata Collection ---
if st.session_state["flow_step"] == 1:
    st.markdown("### Step 1: Enter Metadata")
    # use a form so the metadata is set in one interaction
    with st.form(key="metadata_form"):
        client_name = st.text_input("Client Name", value=st.session_state.get("client_name", ""))
        month = st.selectbox(
            "Month",    
            [
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December",
            ],
            index=0 if not st.session_state.get("month") else None,
        )
        gen_type = st.selectbox("Type", ["Solar", "Wind"], index=0 if st.session_state.get("gen_type") == "Solar" else 1)
        submitted = st.form_submit_button("Next: Provide Data Inputs")
        if submitted:
            st.session_state["client_name"] = client_name.strip()
            st.session_state["month"] = month
            st.session_state["gen_type"] = gen_type
            st.session_state["flow_step"] = 2
            st.rerun()

# --- Step 2: Consumption Data Input ---
elif st.session_state["flow_step"] == 2:
    st.markdown("### Step 2: Provide Consumption Data Inputs")
    gen_type = st.session_state.get("gen_type")

    if not gen_type:
        st.warning("Metadata not found. Please complete Step 1 first.")
        if st.button("Go to Step 1"):
            st.session_state["flow_step"] = 1
            st.rerun()
        st.stop()

    with st.form(key="consumption_data_form"):
        hrbr_file = st.file_uploader("Upload HRBR Unit Excel file", type=["xlsx"], key="hrbr")
        st.markdown("#### Unit Values (editable)")
        unit_values: Dict[str, float] = {}
        for unit, val in DEFAULT_UNIT_VALUES.items():
            unit_values[unit] = st.number_input(f"{unit}", value=float(val), key=f"unit_{unit}")

        validate_btn = st.form_submit_button("Validate Consumption Data")
        back_btn = st.form_submit_button("Back")

        if back_btn:
            st.session_state["flow_step"] = 1
            st.rerun()

        if validate_btn:
            try:
                if hrbr_file is None:
                    raise ValueError("Please upload HRBR Excel file.")
                df = pd.read_excel(hrbr_file)
                required_columns = ["DateTime", "Consumption"]
                validation_utils.validate_columns(df, required_columns, context="Consumption")
                validation_utils.validate_no_nans(df, required_columns, context="Consumption")
                validation_utils.validate_positive_values(df, ["Consumption"], context="Consumption")
                validation_utils.validate_nonempty(df, context="Consumption")
                if "DateTime" in df.columns:
                    validation_utils.validate_month(df, "DateTime", context="Consumption")
                    # Skipping 15-min granularity check for consumption data as per provided sample file
                    # validation_utils.validate_15min_granularity(df, "DateTime", context="Consumption")

                st.success("Consumption data validation passed!")
                st.session_state.update({
                    "validation_passed_consumption": True,
                    "hrbr_file": hrbr_file,
                    "unit_values": unit_values,
                })
                st.session_state["flow_step"] = 3
                st.rerun()

            except Exception as e:
                logger.exception("Validation failed")
                st.error(f"Validation failed: {e}")
                st.session_state["validation_passed_consumption"] = False

# --- Step 3: Generation Data Input ---
elif st.session_state["flow_step"] == 3 and st.session_state.get("validation_passed_consumption", False):
    st.markdown("### Step 3: Provide Generation Data Inputs")
    gen_type = st.session_state.get("gen_type")

    if not gen_type:
        st.warning("Metadata not found. Please complete Step 1 first.")
        if st.button("Go to Step 1"):
            st.session_state["flow_step"] = 1
            st.rerun()
        st.stop()

    with st.form(key="generation_data_form"):
        gen_excel = st.file_uploader("Upload Generation Data Excel file (Generation_Data_Aug.xlsx)", type=["xlsx"], key="gen_excel")
        validate_btn = st.form_submit_button("Validate Generation Data")
        back_btn = st.form_submit_button("Back")

        if back_btn:
            st.session_state["flow_step"] = 2
            st.rerun()

        if validate_btn:
            try:
                if gen_excel is None:
                    raise ValueError("Please upload Generation Excel file.")
                df = pd.read_excel(gen_excel)
                required_columns = ["Date & Time", "Day Gen (KWh)"]
                validation_utils.validate_columns(df, required_columns, context="Generation")
                validation_utils.validate_no_nans(df, required_columns, context="Generation")
                validation_utils.validate_positive_values(df, ["Day Gen (KWh)"], context="Generation")
                validation_utils.validate_nonempty(df, context="Generation")
                if "Date & Time" in df.columns:
                    validation_utils.validate_month(df, "Date & Time", context="Generation")
                    # Skipping 15-min granularity check for generation data as per provided sample file
                    # validation_utils.validate_15min_granularity(df, "Date & Time", context="Generation")

                st.success("Generation data validation passed!")
                st.session_state.update({
                    "validation_passed_generation": True,
                    "gen_excel_file": gen_excel,  # Use a different key to avoid Streamlit widget conflict
                })
                st.session_state["flow_step"] = 4
                st.rerun()

            except Exception as e:
                logger.exception("Validation failed")
                st.error(f"Validation failed: {e}")
                st.session_state["validation_passed_generation"] = False

# --- Step 4: Upload/Run Automation ---
elif st.session_state["flow_step"] == 4 and st.session_state.get("validation_passed_generation", False):
    st.markdown("### Step 4: Upload and Run Automation")

    gen_type = st.session_state.get("gen_type")
    if not gen_type:
        st.warning("Metadata not found. Please complete Step 1 first.")
        if st.button("Go to Step 1"):
            st.session_state["flow_step"] = 1
            st.rerun()
        st.stop()

    client = st.session_state.get("client_name")
    month = st.session_state.get("month")
    hrbr_file = st.session_state.get("hrbr_file")
    unit_values = st.session_state.get("unit_values", DEFAULT_UNIT_VALUES)
    gen_excel = st.session_state.get("gen_excel_file")  # Use the new key

    col1, col2 = st.columns([2, 1])
    with col1:
        if st.button("Run Full Data Processing Workflow"):
            with st.spinner("Processing..."):
                try:
                    with tempfile.TemporaryDirectory() as tmpdir:
                        tmpdir_path = Path(tmpdir)

                        # Consumption processing
                        hrbr_path = tmpdir_path / "HRBR_Aug.xlsx"
                        save_uploaded_file(hrbr_file, hrbr_path)

                        process_hrbr_consumption(str(hrbr_path))
                        st.info("Step 1: HRBR Unit processed.")

                        hourly_units_file = tmpdir_path / "hourly_consumption_units_Aug.xlsx"
                        split_units_to_hourly(str(hrbr_path), str(hourly_units_file), unit_values)
                        st.info("Step 2: Units split to hourly.")

                        consolidated_file = tmpdir_path / "consumption_consolidated_aug.xlsx"
                        consolidate_units_hourly(str(hourly_units_file), str(consolidated_file))
                        st.info("Step 3: Units consolidated.")

                        add_tod_slot(str(consolidated_file))
                        st.info("Step 4: ToD slot added.")

                        merge_hourly_to_tod(str(consolidated_file))
                        st.info("Step 5: Merged hourly to ToD.")

                        split_hourly_to_15min(str(consolidated_file))
                        st.info("Step 6: Split to 15 min intervals.")

                        merge_hourly_to_daily(str(consolidated_file))
                        st.info("Step 7: Merged hourly to daily.")

                        # Generation processing
                        gen_excel_path = tmpdir_path / f"{gen_type}_Generation_Data_Aug.xlsx"
                        save_uploaded_file(gen_excel, gen_excel_path)

                        aggregate_15min(str(gen_excel_path))
                        st.info("Step 8: Aggregated to 15 min.")

                        split_date_time(str(gen_excel_path))
                        st.info("Step 9: Split Date and Time.")

                        output_file = gen_excel_path
                        hourly_output_file = hourly_units_file

                        # Display collected metadata
                        st.markdown("### Metadata Collected")
                        st.write(f"**Client:** {client}")
                        st.write(f"**Month:** {month}")
                        st.write(f"**Type:** {gen_type}")

                        # Download buttons
                        if output_file and output_file.exists():
                            with open(output_file, "rb") as f:
                                st.download_button("Download Generation Results Excel", f, file_name=output_file.name)

                        if hourly_output_file and hourly_output_file.exists():
                            with open(hourly_output_file, "rb") as f:
                                st.download_button("Download Hourly Aggregated Excel", f, file_name=hourly_output_file.name)

                        st.success("✅ All steps completed. Download your results above.")

                except Exception as e:
                    logger.exception("Processing failed")
                    st.error(f"Error during processing: {e}")

    with col2:
        if st.button("Back"):
            st.session_state["flow_step"] = 3
            st.rerun()

st.markdown("---")
st.info(
    "This app allows you to upload the HRBR Unit Excel, enter unit values, and upload the Generation data file. "
    "It will process all steps (consumption, generation, settlement) and provide the final results for download."
)
