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
        st.warning("⚠️ **Metadata Missing:** We couldn't find the required information for this step. Please complete Step 1 (Metadata Entry) first. If you believe this is a mistake, try going back and re-entering your details.")
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
            validation_steps = [
                ("Checking required columns...", lambda df: validation_utils.validate_columns(df, ["DateTime", "Consumption"], context="Consumption")),
                ("Checking for missing values...", lambda df: validation_utils.validate_no_nans(df, ["DateTime", "Consumption"], context="Consumption")),
                ("Checking for positive values...", lambda df: validation_utils.validate_positive_values(df, ["Consumption"], context="Consumption")),
                ("Checking for non-empty data...", lambda df: validation_utils.validate_nonempty(df, context="Consumption")),
                ("Checking month consistency...", lambda df: validation_utils.validate_month(df, "DateTime", context="Consumption") if "DateTime" in df.columns else None),
                # ("Checking 15-min granularity...", lambda df: validation_utils.validate_15min_granularity(df, "DateTime", context="Consumption") if "DateTime" in df.columns else None),
            ]
            validation_results = []
            all_passed = True
            try:
                if hrbr_file is None:
                    raise ValueError("No HRBR Excel file uploaded. Please select and upload the required file before proceeding. Accepted format: .xlsx")
                df = pd.read_excel(hrbr_file)
                for step_msg, check_fn in validation_steps:
                    try:
                        check_fn(df)
                        validation_results.append({"message": step_msg, "status": "passed"})
                    except Exception as ve:
                        # Add more context to the error
                        user_tip = (
                            "Tip: Double-check your file for correct columns, missing values, and data format. "
                            "If the error persists, review the sample/template file or contact support."
                        )
                        validation_results.append({
                            "message": step_msg,
                            "status": "failed",
                            "error": f"{ve} | {user_tip}"
                        })
                        all_passed = False
                        break
                if all_passed:
                    validation_results.append({"message": "✅ All consumption data validations passed!", "status": "final_pass"})
                st.session_state["consumption_validation_results"] = validation_results
                st.session_state["consumption_validation_ready"] = True
                st.session_state["hrbr_file"] = hrbr_file
                st.session_state["unit_values"] = unit_values
                st.session_state["validation_passed_consumption"] = all_passed
            except Exception as e:
                logger.exception("Validation failed")
                user_tip = (
                    "Tip: Ensure your file is not corrupted and follows the required format. "
                    "If you need help, refer to the documentation or contact support."
                )
                validation_results.append({
                    "message": "Validation failed",
                    "status": "failed",
                    "error": f"{e} | {user_tip}"
                })
                st.session_state["consumption_validation_results"] = validation_results
                st.session_state["consumption_validation_ready"] = True
                st.session_state["validation_passed_consumption"] = False

    # Show validation results if available (OUTSIDE the form)
    if st.session_state.get("consumption_validation_ready"):
        for res in st.session_state.get("consumption_validation_results", []):
            if res["status"] == "passed":
                st.success(f"{res['message']} Passed.")
            elif res["status"] == "failed":
                st.error(
                    f"❌ **{res['message']} Failed!**\n\n"
                    f"**Details:** {res.get('error', '')}\n\n"
                    "If you need help, please check the file format, review the error details above, or contact support."
                )
            elif res["status"] == "final_pass":
                st.success(res["message"])
        if st.session_state.get("validation_passed_consumption"):
            if st.button("Continue to Generation Data Input"):
                st.session_state["flow_step"] = 3
                st.session_state["consumption_validation_ready"] = False
                st.rerun()
        else:
            if st.button("Retry Consumption Validation"):
                st.session_state["consumption_validation_ready"] = False
                st.session_state["consumption_validation_results"] = []
                st.rerun()

# --- Step 3: Generation Data Input ---
elif st.session_state["flow_step"] == 3 and st.session_state.get("validation_passed_consumption", False):
    st.markdown("### Step 3: Provide Generation Data Inputs")
    gen_type = st.session_state.get("gen_type")

    if not gen_type:
        st.warning("⚠️ **Metadata Missing:** We couldn't find the required information for this step. Please complete Step 1 (Metadata Entry) first. If you believe this is a mistake, try going back and re-entering your details.")
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
            validation_steps = [
                ("Checking required columns...", lambda df: validation_utils.validate_columns(df, ["Date & Time", "Day Gen (KWh)"], context="Generation")),
                ("Checking for missing values...", lambda df: validation_utils.validate_no_nans(df, ["Date & Time", "Day Gen (KWh)"], context="Generation")),
                ("Checking for positive values...", lambda df: validation_utils.validate_positive_values(df, ["Day Gen (KWh)"], context="Generation")),
                ("Checking for non-empty data...", lambda df: validation_utils.validate_nonempty(df, context="Generation")),
                ("Checking month consistency...", lambda df: validation_utils.validate_month(df, "Date & Time", context="Generation") if "Date & Time" in df.columns else None),
                # ("Checking 15-min granularity...", lambda df: validation_utils.validate_15min_granularity(df, "Date & Time", context="Generation") if "Date & Time" in df.columns else None),
            ]
            validation_results = []
            all_passed = True
            try:
                if gen_excel is None:
                    raise ValueError("No Generation Excel file uploaded. Please select and upload the required file before proceeding. Accepted format: .xlsx")
                df = pd.read_excel(gen_excel)
                for step_msg, check_fn in validation_steps:
                    try:
                        check_fn(df)
                        validation_results.append({"message": step_msg, "status": "passed"})
                    except Exception as ve:
                        user_tip = (
                            "Tip: Double-check your file for correct columns, missing values, and data format. "
                            "If the error persists, review the sample/template file or contact support."
                        )
                        validation_results.append({
                            "message": step_msg,
                            "status": "failed",
                            "error": f"{ve} | {user_tip}"
                        })
                        all_passed = False
                        break
                if all_passed:
                    validation_results.append({"message": "✅ All generation data validations passed!", "status": "final_pass"})
                st.session_state["generation_validation_results"] = validation_results
                st.session_state["generation_validation_ready"] = True
                st.session_state["gen_excel_file"] = gen_excel
                st.session_state["validation_passed_generation"] = all_passed
            except Exception as e:
                logger.exception("Validation failed")
                user_tip = (
                    "Tip: Ensure your file is not corrupted and follows the required format. "
                    "If you need help, refer to the documentation or contact support."
                )
                validation_results.append({
                    "message": "Validation failed",
                    "status": "failed",
                    "error": f"{e} | {user_tip}"
                })
                st.session_state["generation_validation_results"] = validation_results
                st.session_state["generation_validation_ready"] = True
                st.session_state["validation_passed_generation"] = False

    # Show validation results if available (OUTSIDE the form)
    if st.session_state.get("generation_validation_ready"):
        for res in st.session_state.get("generation_validation_results", []):
            if res["status"] == "passed":
                st.success(f"{res['message']} Passed.")
            elif res["status"] == "failed":
                st.error(
                    f"❌ **{res['message']} Failed!**\n\n"
                    f"**Details:** {res.get('error', '')}\n\n"
                    "If you need help, please check the file format, review the error details above, or contact support."
                )
            elif res["status"] == "final_pass":
                st.success(res["message"])
        if st.session_state.get("validation_passed_generation"):
            if st.button("Continue to Automation Step"):
                st.session_state["flow_step"] = 4
                st.session_state["generation_validation_ready"] = False
                st.rerun()
        else:
            if st.button("Retry Generation Validation"):
                st.session_state["generation_validation_ready"] = False
                st.session_state["generation_validation_results"] = []
                st.rerun()

# --- Step 4: Upload/Run Automation ---
elif st.session_state["flow_step"] == 4 and st.session_state.get("validation_passed_generation", False):
    st.markdown("### Step 4: Upload and Run Automation")

    gen_type = st.session_state.get("gen_type")
    if not gen_type:
        st.warning("⚠️ **Metadata Missing:** We couldn't find the required information for this step. Please complete Step 1 (Metadata Entry) first. If you believe this is a mistake, try going back and re-entering your details.")
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
            # --- Show all steps as pending first ---
            processing_steps = [
                # (Description, function, args, kwargs)
                ("Saving HRBR file", save_uploaded_file, [hrbr_file, None], {}),  # dest_path to be set later
                ("Processing HRBR Unit", process_hrbr_consumption, [None], {}),  # path to be set later
                ("Splitting units to hourly", split_units_to_hourly, [None, None, unit_values], {}),  # paths to be set later
                ("Consolidating units hourly", consolidate_units_hourly, [None, None], {}),
                ("Adding ToD slot", add_tod_slot, [None], {}),
                ("Merging hourly to ToD", merge_hourly_to_tod, [None], {}),
                ("Splitting hourly to 15min", split_hourly_to_15min, [None], {}),
                ("Merging hourly to daily", merge_hourly_to_daily, [None], {}),
                ("Saving Generation file", save_uploaded_file, [gen_excel, None], {}),  # dest_path to be set later
                ("Aggregating to 15min", aggregate_15min, [None], {}),
                ("Splitting Date and Time", split_date_time, [None], {}),
                ("Merging Generation and Consumption", merge_generation_consumption, [None, None, None], {}),
                ("Aggregating to hourly", aggregate_hourly, [None, None], {}),
                ("Calculating Matched Settlement", calculate_matched_settlement, [None, "15 mins", "matched_settlement"], {}),
                ("Adding Unit ID", add_unit_id, [None, "matched_settlement", "matched_settlement_with_id"], {}),
                ("Monthly Aggregation", monthly_aggregation, [None, "matched_settlement_with_id", "monthly"], {}),
                ("Applying Monthly Banking Settlement", apply_monthly_banking_settlement, [None, "monthly", "banking_settlement"], {}),
                ("Calculating Savings Comparison", calculate_savings_comparison, [], {
                    "input_file": None,
                    "sheet_name": "banking_settlement",
                    "high_grid_rate_per_kwh": 7.20,
                    "low_grid_rate_per_kwh": 5.95,
                    "renewable_rate_per_kwh": 1.0,
                    "output_sheet": "monthly_saving"
                }),
            ]
            # Create a placeholder for each step
            step_placeholders = []
            st.markdown("#### Workflow Progress")
            for step in processing_steps:
                ph = st.empty()
                ph.info(f"⏳ {step[0]} ... Pending")
                step_placeholders.append(ph)

            processing_results = []
            all_passed = True
            try:
                with st.spinner("Processing..."):
                    import time as _time
                    with tempfile.TemporaryDirectory() as tmpdir:
                        tmpdir_path = Path(tmpdir)
                        # Prepare file paths
                        hrbr_path = tmpdir_path / "HRBR_Aug.xlsx"
                        hourly_units_file = tmpdir_path / "hourly_consumption_units_Aug.xlsx"
                        consolidated_file = tmpdir_path / "consumption_consolidated_aug.xlsx"
                        gen_excel_path = tmpdir_path / f"{gen_type}_Generation_Data_Aug.xlsx"
                        merged_consumption_generation_file = tmpdir_path / "Consumption_Generation_Aug25.xlsx"
                        hourly_merged_file = tmpdir_path / "Consumption_Generation_Aug25_hourly.xlsx"

                        # Patch args for steps that need file paths
                        # Save HRBR
                        processing_steps[0][2][1:] = [hrbr_path]
                        # Process HRBR
                        processing_steps[1][2][0] = str(hrbr_path)
                        # Split units to hourly
                        processing_steps[2][2][0] = str(hrbr_path)
                        processing_steps[2][2][1] = str(hourly_units_file)
                        # Consolidate units hourly
                        processing_steps[3][2][0] = str(hourly_units_file)
                        processing_steps[3][2][1] = str(consolidated_file)
                        # Add ToD slot
                        processing_steps[4][2][0] = str(consolidated_file)
                        # Merge hourly to ToD
                        processing_steps[5][2][0] = str(consolidated_file)
                        # Split hourly to 15min
                        processing_steps[6][2][0] = str(consolidated_file)
                        # Merge hourly to daily
                        processing_steps[7][2][0] = str(consolidated_file)
                        # Save Generation file
                        processing_steps[8][2][1:] = [gen_excel_path]
                        # Aggregate 15min
                        processing_steps[9][2][0] = str(gen_excel_path)
                        # Split Date and Time
                        processing_steps[10][2][0] = str(gen_excel_path)
                        # Merge Generation and Consumption
                        processing_steps[11][2][0] = str(gen_excel_path)
                        processing_steps[11][2][1] = str(consolidated_file)
                        processing_steps[11][2][2] = str(merged_consumption_generation_file)
                        # Aggregate to hourly
                        processing_steps[12][2][0] = str(merged_consumption_generation_file)
                        processing_steps[12][2][1] = str(hourly_merged_file)
                        # Matched Settlement
                        processing_steps[13][2][0] = str(merged_consumption_generation_file)
                        # Add Unit ID
                        processing_steps[14][2][0] = str(merged_consumption_generation_file)
                        # Monthly Aggregation
                        processing_steps[15][2][0] = str(merged_consumption_generation_file)
                        # Banking Settlement
                        processing_steps[16][2][0] = str(merged_consumption_generation_file)
                        # Savings Comparison
                        processing_steps[17][3]["input_file"] = str(merged_consumption_generation_file)

                        # Run each step and update UI in real-time
                        for idx, step in enumerate(processing_steps):
                            step_msg, fn, args, kwargs = step
                            try:
                                fn(*args, **kwargs)
                                step_placeholders[idx].success(f"✅ {step_msg} Passed.")
                                processing_results.append({"message": step_msg, "status": "passed"})
                            except Exception as e:
                                user_tip = (
                                    "Tip: Please check the input files and formats. "
                                    "If the error persists, contact support."
                                )
                                step_placeholders[idx].error(
                                    f"❌ {step_msg} Failed!\n\n"
                                    f"**Details:** {e} | {user_tip}"
                                )
                                processing_results.append({
                                    "message": step_msg,
                                    "status": "failed",
                                    "error": f"{e} | {user_tip}"
                                })
                                all_passed = False
                                # Mark remaining steps as not run
                                for j in range(idx + 1, len(processing_steps)):
                                    step_placeholders[j].warning(f"⚠️ {processing_steps[j][0]} Not Run.")
                                break
                            # Optional: add a small delay for better UX
                            # _time.sleep(0.2)

                        st.session_state["processing_results"] = processing_results
                        st.session_state["processing_ready"] = True
                        st.session_state["processing_all_passed"] = all_passed
                        # Store file paths for download in session state for later use
                        st.session_state["processing_output_file"] = str(gen_excel_path)
                        st.session_state["processing_hourly_output_file"] = str(hourly_units_file)
                        st.session_state["processing_merged_consumption_generation_file"] = str(merged_consumption_generation_file)
                        st.session_state["processing_tmpdir_path"] = str(tmpdir_path)
                        st.session_state["processing_client"] = client
                        st.session_state["processing_month"] = month
                        st.session_state["processing_gen_type"] = gen_type

                        if all_passed:
                            # Display collected metadata
                            st.markdown("### Metadata Collected")
                            st.write(f"**Client:** {client}")
                            st.write(f"**Month:** {month}")
                            st.write(f"**Type:** {gen_type}")

                            output_file = gen_excel_path
                            hourly_output_file = hourly_units_file

                            # Download buttons
                            if output_file and output_file.exists():
                                with open(output_file, "rb") as f:
                                    st.download_button("Download Generation Results Excel", f, file_name=output_file.name)

                            if hourly_output_file and hourly_output_file.exists():
                                with open(hourly_output_file, "rb") as f:
                                    st.download_button("Download Hourly Aggregated Excel", f, file_name=hourly_output_file.name)

                            # Settlement output downloads
                            if merged_consumption_generation_file.exists():
                                with open(merged_consumption_generation_file, "rb") as f:
                                    st.download_button("Download Consumption-Generation Excel", f, file_name=merged_consumption_generation_file.name)
                                # Download monthly_saving
                                import openpyxl
                                try:
                                    wb = openpyxl.load_workbook(str(merged_consumption_generation_file), read_only=True)
                                    if "monthly_saving" in wb.sheetnames:
                                        # Save monthly_saving to a temp file for download
                                        monthly_saving_file = tmpdir_path / "monthly_saving.xlsx"
                                        df_monthly_saving = pd.read_excel(merged_consumption_generation_file, sheet_name="monthly_saving")
                                        df_monthly_saving.to_excel(monthly_saving_file, index=False)
                                        with open(monthly_saving_file, "rb") as f2:
                                            st.download_button("Download Monthly Saving Excel", f2, file_name=monthly_saving_file.name)
                                    wb.close()
                                except Exception as e:
                                    st.warning(
                                        f"⚠️ **Could not prepare monthly_saving download.**\n\n"
                                        f"**Details:** {e}\n\n"
                                        "Please check if the 'monthly_saving' sheet exists in the file. If the issue persists, contact support."
                                    )

                            # --- Save all files to a project folder ---
                            import shutil
                            import time

                            # Create a unique project folder name
                            safe_client = "".join(c for c in str(client) if c.isalnum() or c in (" ", "_", "-")).replace(" ", "_")
                            safe_month = "".join(c for c in str(month) if c.isalnum() or c in (" ", "_", "-")).replace(" ", "_")
                            timestamp = time.strftime("%Y%m%d_%H%M%S")
                            project_folder = Path("Final Files") / f"{safe_client}_{safe_month}_{timestamp}"
                            project_folder.mkdir(parents=True, exist_ok=True)

                            # Save uploaded files
                            if hrbr_file is not None:
                                hrbr_dest = project_folder / "HRBR_Uploaded.xlsx"
                                save_uploaded_file(hrbr_file, hrbr_dest)
                            if gen_excel is not None:
                                gen_dest = project_folder / "Generation_Uploaded.xlsx"
                                save_uploaded_file(gen_excel, gen_dest)

                            # Copy all files created in tmpdir_path to project_folder
                            for file_path in tmpdir_path.glob("*"):
                                if file_path.is_file():
                                    shutil.copy(file_path, project_folder / file_path.name)

                            # Optionally, zip the project folder and provide a download link
                            zip_path = project_folder.with_suffix(".zip")
                            shutil.make_archive(str(project_folder), 'zip', root_dir=project_folder)

                            st.info(f"All uploaded and generated files have been saved to: {project_folder.resolve()}")
                            with open(zip_path, "rb") as f:
                                st.download_button("Download All Project Files (ZIP)", f, file_name=zip_path.name)

                            st.success("✅ All steps completed. Download your results above.")

            except Exception as e:
                logger.exception("Processing failed")
                processing_results.append({
                    "message": "Processing failed",
                    "status": "failed",
                    "error": f"{e} | Tip: Please check the input files and formats. If the error persists, contact support."
                })
                st.session_state["processing_results"] = processing_results
                st.session_state["processing_ready"] = True

    with col2:
        if st.button("Back"):
            st.session_state["flow_step"] = 3
            st.rerun()

    # Show processing results if available (OUTSIDE the button)
    if st.session_state.get("processing_ready"):
        for res in st.session_state.get("processing_results", []):
            if res["status"] == "passed":
                st.success(f"{res['message']} Passed.")
            elif res["status"] == "failed":
                st.error(
                    f"❌ **{res['message']} Failed!**\n\n"
                    f"**Details:** {res.get('error', '')}\n\n"
                    "If you need help, please check the file format, review the error details above, or contact support."
                )
        # Reset processing_ready after displaying results
        if st.button("Clear Processing Results"):
            st.session_state["processing_ready"] = False
            st.session_state["processing_results"] = []

st.markdown("---")
st.info(
    "This app allows you to upload the HRBR Unit Excel, enter unit values, and upload the Generation data file. "
    "It will process all steps (consumption, generation, settlement) and provide the final results for download."
)
