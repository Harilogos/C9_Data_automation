import pandas as pd
import os

def calculate_discom_bill(
        unit, 
        month,
        grid_cost_rate, 
        renewable_cost_rate, 
        total_consumption, 
        with_banking_consumption, 
        demand_charges_tariff, 
        demand_charges_kwh,
        fuel_cost_adj_charges_tariff,
        tax_tariff,
        PandG_surcharge_tariff,
        manual_wheeling_energy_charge_tariff,
        manual_energy_charge_tariff,
        output_file="discom_bill_aug.xlsx"
    ):
    # ------------------- Total Consumption-------------------------
    total_consumption_tariff = grid_cost_rate
    total_consumption_kwh = total_consumption
    total_consumption_cost_wo_solar = total_consumption_tariff * total_consumption_kwh
    total_consumption_cost_with_solar = 0
    total_consumption_discom_bill = 0
    total_consumption_saving = 0

    # ------------ Wheeling energy - with banking ---------------
    wheeling_energy_with_banking_tariff = renewable_cost_rate
    wheeling_energy_with_banking_kwh = with_banking_consumption
    wheeling_energy_with_banking_cost_wo_solar = 0
    wheeling_energy_with_banking_cost_with_solar = wheeling_energy_with_banking_tariff * wheeling_energy_with_banking_kwh
    wheeling_energy_with_banking_discom_bill = 0
    wheeling_energy_with_banking_saving = 0

    # ------------ Energy charges ---------------
    energy_charges_tariff = grid_cost_rate
    energy_charges_kwh = total_consumption_kwh - wheeling_energy_with_banking_kwh
    energy_charges_cost_wo_solar = 0
    energy_charges_cost_with_solar = energy_charges_tariff * energy_charges_kwh
    energy_charges_discom_bill = energy_charges_cost_with_solar
    energy_charges_saving = 0

    # ------------ Demand charges – fixed -----------------
    demand_charges_cost_wo_solar = demand_charges_tariff * demand_charges_kwh
    demand_charges_cost_with_solar = demand_charges_cost_wo_solar
    demand_charges_discom_bill = demand_charges_cost_with_solar
    demand_charges_saving = 0

    # ----------- Fuel Cost Adjustment Charges - fixed --------------
    fuel_cost_adj_charges_cost_wo_solar = total_consumption_kwh * fuel_cost_adj_charges_tariff
    fuel_cost_adj_charges_kwh = energy_charges_kwh
    fuel_cost_adj_charges_cost_with_solar = fuel_cost_adj_charges_tariff * fuel_cost_adj_charges_kwh
    fuel_cost_adj_charges_discom = fuel_cost_adj_charges_cost_with_solar
    fuel_cost_adj_charges_saving = 0

    # ----------------- Tax  fixed ---------------
    tax_cost_wo_solar = tax_tariff * total_consumption_cost_wo_solar
    tax_cost_with_solar = tax_tariff * energy_charges_cost_with_solar
    tax_discom_bill = tax_cost_with_solar
    tax_tariff_saving = 0

    # ---------------- P&G surcharge – fixed ---------------
    PandG_surcharge_kwh = energy_charges_kwh
    PandG_surcharge_cost_wo_solar = PandG_surcharge_tariff * total_consumption_kwh
    PandG_surcharge_cost_with_solar = PandG_surcharge_tariff * PandG_surcharge_kwh
    PandG_surcharge_discom_bill = PandG_surcharge_cost_with_solar
    PandG_surcharge_saving = 0

    # ------------ Manual wheeling Energy charge - fixed ----------
    manual_wheeling_energy_charge_kwh = wheeling_energy_with_banking_kwh
    manual_wheeling_energy_charge_cost_wo_solar = 0
    manual_wheeling_energy_charge_cost_with_solar = manual_wheeling_energy_charge_tariff * manual_wheeling_energy_charge_kwh
    manual_wheeling_energy_charge_discom_bill = manual_wheeling_energy_charge_cost_with_solar
    manual_wheeling_energy_charge_saving = 0

    # -------- Manual energy charges – fixed ( Wheeling) ----------
    manual_energy_charge_kwh = wheeling_energy_with_banking_kwh
    manual_energy_charge_cost_wo_solar = 0
    manual_energy_charge_cost_with_solar = manual_energy_charge_tariff * manual_energy_charge_kwh
    manual_energy_charge_discom_bill = manual_energy_charge_cost_with_solar 
    manual_energy_charge_saving = 0
    
    # ----------------- Net Payable ---------------------
    net_payable_cost_wo_solar = sum([
        demand_charges_cost_wo_solar,
        fuel_cost_adj_charges_cost_wo_solar,
        PandG_surcharge_cost_wo_solar,
        tax_cost_wo_solar,
        total_consumption_cost_wo_solar
    ])
    net_payable_cost_with_solar = sum([
        demand_charges_cost_with_solar,
        fuel_cost_adj_charges_cost_with_solar,
        PandG_surcharge_cost_with_solar,
        tax_cost_with_solar,
        energy_charges_cost_with_solar,
        wheeling_energy_with_banking_cost_with_solar,
        manual_energy_charge_cost_with_solar,
        manual_wheeling_energy_charge_cost_with_solar
    ])
    net_payable_discom_bill = sum([
        demand_charges_discom_bill,
        fuel_cost_adj_charges_discom,
        PandG_surcharge_discom_bill,
        tax_discom_bill,
        energy_charges_discom_bill,
        manual_energy_charge_discom_bill,
        manual_wheeling_energy_charge_discom_bill
    ])
    net_payable_saving = net_payable_cost_wo_solar - net_payable_cost_with_solar

    # ---------------- Build DataFrame ----------------
    data = [
        ["Total Consumption", unit, month, total_consumption_tariff, total_consumption_kwh, total_consumption_cost_wo_solar, total_consumption_cost_with_solar, total_consumption_discom_bill, total_consumption_saving],
        ["Wheeling Energy", unit, month, wheeling_energy_with_banking_tariff, wheeling_energy_with_banking_kwh, wheeling_energy_with_banking_cost_wo_solar, wheeling_energy_with_banking_cost_with_solar, wheeling_energy_with_banking_discom_bill, wheeling_energy_with_banking_saving],
        ["Energy Charges", unit, month, energy_charges_tariff, energy_charges_kwh, energy_charges_cost_wo_solar, energy_charges_cost_with_solar, energy_charges_discom_bill, energy_charges_saving],
        ["Demand Charges – Fixed", unit, month, demand_charges_tariff, demand_charges_kwh, demand_charges_cost_wo_solar, demand_charges_cost_with_solar, demand_charges_discom_bill, demand_charges_saving],
        ["Fuel Cost Adjustment Charges - Fixed", unit, month, fuel_cost_adj_charges_tariff, fuel_cost_adj_charges_kwh, fuel_cost_adj_charges_cost_wo_solar, fuel_cost_adj_charges_cost_with_solar, fuel_cost_adj_charges_discom, fuel_cost_adj_charges_saving],
        ["Tax – Fixed", unit, month, tax_tariff, "-", tax_cost_wo_solar, tax_cost_with_solar, tax_discom_bill, tax_tariff_saving],
        ["P&G Surcharge – Fixed", unit, month, PandG_surcharge_tariff, PandG_surcharge_kwh, PandG_surcharge_cost_wo_solar, PandG_surcharge_cost_with_solar, PandG_surcharge_discom_bill, PandG_surcharge_saving],
        ["Manual Wheeling Energy Charge - Fixed", unit, month, manual_wheeling_energy_charge_tariff, manual_wheeling_energy_charge_kwh, manual_wheeling_energy_charge_cost_wo_solar, manual_wheeling_energy_charge_cost_with_solar, manual_wheeling_energy_charge_discom_bill, manual_wheeling_energy_charge_saving],
        ["Manual Energy Charges – Fixed ( Wheeling)", unit, month, manual_energy_charge_tariff, manual_energy_charge_kwh, manual_energy_charge_cost_wo_solar, manual_energy_charge_cost_with_solar, manual_energy_charge_discom_bill, manual_energy_charge_saving],
        ["Net Payable", unit, month, "-", "-", net_payable_cost_wo_solar, net_payable_cost_with_solar, net_payable_discom_bill, net_payable_saving]
    ]
    df = pd.DataFrame(data, columns=[
        "Bill headers", "Unit", "Month & Year", "Tariff", "kWh/kW", 
        "Cost without solar", "Cost with Solar wheeling", 
        "DISCOM Bill", "Savings (C-D)"
    ])

    # ---------------- Save or Append to Excel ----------------
    if os.path.exists(output_file):
        existing_df = pd.read_excel(output_file)
        final_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        final_df = df
    final_df.to_excel(output_file, index=False)
    return df

def run_billing_automation():
    # AUGUST 2025
    month = "2025-08"
    renewable_cost_rate = 1
    fuel_cost_adj_charges_tariff = 0.39
    tax_tariff = 0.09
    PandG_surcharge_tariff = 0.36
    manual_wheeling_energy_charge_tariff = 0.32
    manual_energy_charge_tariff = 0.2
    units_data = [
        ("MALLESWARAM (C2HT-136)", 7.20, 48359.985, 40990, 350, 180),
        ("SAHAKAR NAGAR (C8HT-111)", 7.20, 58407.5, 52786, 350, 355),
        ("THANISANDRA (C8HT-135)", 5.95, 53563.019, 46193, 370, 180),
        ("WHITEFIELD (E4HT-355)", 5.95, 88540.058, 81230, 370, 360),
        ("OLD AIRPORT ROAD (E6HT209)", 7.20, 77528.014, 70158, 350, 275),
        ("HRBR UNIT (E8HT-203)", 7.20, 45320, 38925, 350, 180),
        ("BELLANDUR CORP. OFFICE (S11BHT 406)", 5.95, 22886.238, 0, 370, 135),
        ("BELLANDUR (S11HT-124)", 5.95, 48752.24325, 41383, 370, 135),
        ("SARJAPURA (S11HT-419)", 5.95, 45603.012, 38233, 370, 270),
        ("KANAKAPURA (S12HT-99)", 5.95, 45733.521, 38480, 370, 135),
        ("ELECTRONIC CITY (S13HT-87)", 5.95, 69740, 62322, 370, 180),
    ]
    for unit, grid_cost_rate, total_consumption, with_banking_consumption, demand_charges_tariff, demand_charges_kwh in units_data:
        calculate_discom_bill(
            unit,
            month,
            grid_cost_rate,
            renewable_cost_rate,
            total_consumption,
            with_banking_consumption,
            demand_charges_tariff,
            demand_charges_kwh,
            fuel_cost_adj_charges_tariff,
            tax_tariff,
            PandG_surcharge_tariff,
            manual_wheeling_energy_charge_tariff,
            manual_energy_charge_tariff,
            output_file="discom_bill_aug.xlsx"
        )

if __name__ == "__main__":
    run_billing_automation()
