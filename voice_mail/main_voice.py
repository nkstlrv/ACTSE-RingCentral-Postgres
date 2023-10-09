import json
import pretty_errors
from pprint import pprint
from db.db_handler import PostgresHandler
from calls_records.main_records import DB_CONFIG, open_test_payload


def write_vm_result_array(payload: list) -> None:
    with open("vm_result.json", "w") as fw:
        json.dump(payload, fw)
        print("Voice mail JSON (Array) created")


def parse_vm_to_list(payload: dict) -> list:
    voice: list = payload["value"]["ring_central"]["records"]
    result = list()

    for item in voice:
        insert_parameters = {
            "vm_id": item["id"],
            "to_name": item["to"][-1].get("name"),
            "to_location": item["to"][-1].get("location"),
            "from_name": item["from"].get("name"),
            "from_phone_nuber": item["from"].get("phoneNumber"),
            "from_location": item["from"].get("location"),
            "contact_availability": item["availability"],
            "voice_direction": item["direction"],
            "vm_type": item["type"],
            "vm_status": item["messageStatus"],
            "vm_read_status": item["readStatus"],
            "vm_priority": item["priority"],
            "vm_duration": item["attachments"][-1].get("vmDuration"),
            "vm_time": item["creationTime"],
            "vm_time_modified": item["lastModifiedTime"],
            "vm_transcription_status": item.get("vmTranscriptionStatus"),
            "file_uri": item["attachments"][-1].get("uri"),
            "file_type": item["attachments"][-1].get("type"),
            "file_format": item["attachments"][-1].get("contentType"),
            "file_name": item["attachments"][-1].get("fileName"),


        }
        result.append(insert_parameters)

    pprint(result)
    return result


if __name__ == "__main__":
    input_data = open_test_payload("vm_payload.json")

    # # Array of objects format
    parsed_vm_list = parse_vm_to_list(input_data)
    write_vm_result_array(parsed_vm_list)
