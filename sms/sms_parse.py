import json
from db.db_handler import PostgresHandler
from calls_records.call_records_parse import DB_CONFIG, open_test_payload, write_records_result_array


def parse_sms_to_list(payload: dict) -> list:
    sms: list = payload["value"]["ring_central"]["records"]
    result = list()

    for item in sms:
        insert_parameters = {
            "sms_id": item["id"],
            "sms_message": item["subject"],
            "to_name": item["to"][-1].get("name"),
            "to_location": item["to"][-1].get("location"),
            "to_availability": item["availability"],
            "from_name": item["from"].get("name"),
            "from_phone_nuber": item["from"].get("phoneNumber"),
            "from_location": item["from"].get("location"),
            "sms_message_type": item["type"],
            "sms_message_status": item["messageStatus"],
            "sms_direction": item["direction"],
            "sms_read_status": item["readStatus"],
            "sms_priority": item["priority"],
            "sms_time": item["creationTime"],
            "sms_time_modified": item["lastModifiedTime"],
            "conversation_id": item["conversationId"],
            "conversation_uri": item["conversation"].get("uri")

        }

        result.append(insert_parameters)

    print(result)
    return result


if __name__ == "__main__":
    input_data = open_test_payload("sms_ringcentral_payload.json")

    # # Array of objects format
    parsed_records_list = parse_sms_to_list(input_data)
    # write_records_result_array(parsed_records_list)
