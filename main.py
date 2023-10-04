import json
import time
import pretty_errors
from db_handler import PostgresHandler

DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}


def open_test_payload(file_path: str) -> dict:
    with open(file_path, "r") as fr:
        result = json.load(fr)
        return result


def write_records_result_array(payload: list) -> None:
    with open("records_result_list.json", "w") as fw:
        json.dump(payload, fw)
        print("Records JSON (Array) created")


def write_records_result_object(payload: dict) -> None:
    with open("records_result_object.json", "w") as fw:
        json.dump(payload, fw)
        print("Records JSON (Object) created")


def save_record_to_postgres(record: dict) -> None:
    ...


def parse_records_to_list(payload: dict) -> list:
    records: list = payload["value"]["ring_central"]["records"]
    result = list()

    with PostgresHandler(DB_CONFIG) as db_handler:
        create_tabel_query = """
            CREATE TABLE IF NOT EXISTS ringcentral_records (
            id serial PRIMARY KEY,
            call_id VARCHAR(255) UNIQUE,
            session_id VARCHAR(255),
            telephony_session_id VARCHAR(255),
            party_id VARCHAR(255),
            to_name VARCHAR(255),
            to_phone_number VARCHAR(255),
            to_location VARCHAR(255),
            from_name VARCHAR(255),
            from_phone_number VARCHAR(255), 
            from_location VARCHAR(255),
            call_type VARCHAR(255),
            call_direction VARCHAR(255),
            call_result VARCHAR(255),
            call_start_time VARCHAR(255),
            call_duration_seconds VARCHAR(255),
            call_action VARCHAR(255),
            call_internal_type VARCHAR(255),
            call_reason VARCHAR(255),
            call_reason_description TEXT,
            recording_id VARCHAR(255),
            recording_uri VARCHAR(255),
            recording_type VARCHAR(255)
        );

        """
        db_handler.execute(create_tabel_query)

        for item in records:

            insert_parameters = {
                "call_id": item["id"],
                "session_id": item["sessionId"],
                "telephony_session_id": item["telephonySessionId"],
                "party_id": item.get("partyId"),
                "to_name": item["to"].get("name"),
                "to_phone_number": item["to"].get("phoneNumber"),
                "to_location": item["to"].get("location"),
                "from_name": item["from"].get("name"),
                "from_phone_number": item["from"].get("phoneNumber"),
                "from_location": item["from"].get("location"),
                "call_type": item["type"],
                "call_direction": item["direction"],
                "call_result": item["result"],
                "call_start_time": item["startTime"],
                "call_duration_seconds": item["duration"],
                "call_action": item["action"],
                "call_internal_type": item["internalType"],
                "call_reason": item.get("reason"),
                "call_reason_description": item.get("reasonDescription"),
                "recording_id": None,
                "recording_uri": None, "recording_type": None
            }

            if "recording" in item:
                insert_parameters["recording_id"] = item["recording"].get("id")
                insert_parameters["recording_uri"] = item["recording"].get("uri")
                insert_parameters["recording_type"] = item["recording"].get("type")

            result.append(insert_parameters)

            print(insert_parameters)

            insert_query = """
                INSERT INTO ringcentral_records 
                (call_id, 
                session_id, 
                telephony_session_id,
                party_id,
                to_name,
                to_phone_number,
                to_location,
                from_name,
                from_phone_number,
                from_location,
                call_type,
                call_direction,
                call_result,
                call_start_time,
                call_duration_seconds,
                call_action,
                call_internal_type,
                call_reason,
                call_reason_description,
                recording_id,
                recording_uri,
                recording_type
                
                )
                VALUES (
                %(call_id)s, 
                %(session_id)s, 
                %(telephony_session_id)s, 
                %(party_id)s, 
                %(to_name)s,
                %(to_phone_number)s,
                %(to_location)s,
                %(from_name)s,
                %(from_phone_number)s,
                %(from_location)s,
                %(call_type)s,
                %(call_direction)s,
                %(call_result)s,
                %(call_start_time)s,
                %(call_duration_seconds)s,
                %(call_action)s,
                %(call_internal_type)s,
                %(call_reason)s,
                %(call_reason_description)s,
                %(recording_id)s,
                %(recording_uri)s,
                %(recording_type)s
                
                
                );
            """

            db_handler.execute(insert_query, insert_parameters)
            print(f"INSERTING {insert_parameters['call_id']}")

        return result


def parse_records_to_dict(payload: dict) -> dict:
    records: list = payload["value"]["ring_central"]["records"]
    result = dict()

    for item in records:
        result[item["id"]] = {
            "call_id": item["id"],
            "session_id": item["sessionId"],
            "telephony_session_id": item["telephonySessionId"],
            "to_name": item["to"].get("name"),
            "to_location": item["to"].get("location"),
            "from_name": item["from"].get("name"),
            "from_phone_nuber": item["from"].get("phoneNumber"),
            "from_location": item["from"].get("location"),
            "call_type": item["type"],
            "call_direction": item["direction"],
            "call_result": item["result"],
            "call_start_time": item["startTime"],
            "call_duration_seconds": item["duration"],
            "call_action": item["action"],
            "call_internal_type": item["internalType"],
            "call_reason": item.get("reason"),
            "call_reason_description": item.get("reasonDescription"),
        }

    return result


if __name__ == "__main__":
    input_data = open_test_payload("ringcentral_data.json")

    # Array of objects format
    parsed_records_list = parse_records_to_list(input_data)
    write_records_result_array(parsed_records_list)

    # Object of objects format
    parsed_records_object = parse_records_to_dict(input_data)
    write_records_result_object(parsed_records_object)
