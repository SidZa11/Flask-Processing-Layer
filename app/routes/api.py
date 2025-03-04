from flask import Blueprint, jsonify, request
from ..controllers import get_combined_dataset
from ..controllers import section1, section2

api_bp = Blueprint("api", __name__)

@api_bp.route("/health", methods=["GET"])
def get_health():
    print("Health endpoint hit!")
    return "API is Healthy :)"

@api_bp.route("/data", methods=["GET"])
def get_data():
    dataset = get_combined_dataset()
    return jsonify(dataset)

@api_bp.route("/dailyReport", methods=["POST"])
def get_daily_report():
    body = request.get_json()
    print("body", body)
    # body = {
    #     "section1" : {
    #         "ids" : ["e4b61510-cce5-11ef-aad0-5f8aea70db1f", "e8324380-cce5-11ef-aad0-5f8aea70db1f"],
    #         "keys" : ['INV_Total_Power', 'PV_Daily_Energy_Today', 'PV_Total_Energy_kWh', 'DG_Total_Power', 'AC_Active_Power_Watt', 'kWhcharged_Day', 'kWhcharged_Day', 'ESS_SOC'],
    #         "startTs" : 1740441600000,
    #         "endTs" : 1740528000000
    #     },
    #     "section2" : {
    #         "ids" : ["e7fd9ef0-cce5-11ef-aad0-5f8aea70db1f",
    #         "e7ccf200-cce5-11ef-aad0-5f8aea70db1f",
    #         "e78847e0-cce5-11ef-aad0-5f8aea70db1f",
    #         "e750bd20-cce5-11ef-aad0-5f8aea70db1f",
    #         "e7228130-cce5-11ef-aad0-5f8aea70db1f",
    #         "e6ef1520-cce5-11ef-aad0-5f8aea70db1f",
    #         "e6b9d450-cce5-11ef-aad0-5f8aea70db1f",
    #         "e67f1540-cce5-11ef-aad0-5f8aea70db1f",
    #         "e6514e80-cce5-11ef-aad0-5f8aea70db1f",
    #         "e61cd100-cce5-11ef-aad0-5f8aea70db1f",
    #         "e5e60990-cce5-11ef-aad0-5f8aea70db1f",
    #         "e5b8b800-cce5-11ef-aad0-5f8aea70db1f",
    #         "e5830200-cce5-11ef-aad0-5f8aea70db1f",
    #         "e54e3660-cce5-11ef-aad0-5f8aea70db1f",
    #         "e51720d0-cce5-11ef-aad0-5f8aea70db1f",
    #         "e4e896c0-cce5-11ef-aad0-5f8aea70db1f",],
    #         "keys" : ['AC_Active_Power_Watt', 'AC_Reactive_Power_var', 'Energy_Daily_kWh', 'Energy_Total_kWh'],
    #         "startTs" : 1740441600000,
    #         "endTs" : 1740528000000
    #     }
    # }
    if not body or 'section1' not in body or 'section2' not in body:
        return jsonify({"error": "Missing required fields: section1 and section2"}), 400
    
    first_section = section1(body['section1'])
    second_section = section2(body["section2"])

    dataset = {
        "section1" : first_section,
        "section2" : second_section
    }
    return dataset