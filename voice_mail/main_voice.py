import json
import pretty_errors
from pprint import pprint
from db.db_handler import PostgresHandler
from calls_records.main_records import DB_CONFIG, open_test_payload


def write_voice_result_array(payload: list) -> None:
    with open("voice_result_list.json", "w") as fw:
        json.dump(payload, fw)
        print("Voice JSON (Array) created")


def parse_voice_to_list(payload: dict) -> list:
    voice: list = payload["value"]["ring_central"]["records"]
    result = list()

    for item in voice:
        insert_parameters = {
            "voice_id": item["id"],
            "to_name": item["to"][-1].get("name"),
            "to_location": item["to"][-1].get("location"),
            "from_name": item["from"].get("name"),
            "from_phone_nuber": item["from"].get("phoneNumber"),
            "from_location": item["from"].get("location"),
            "contact_availability": item["availability"],
            "voice_direction": item["direction"],
            "voice_type": item["type"],
            "voice_status": item["messageStatus"],
            "voice_read_status": item["readStatus"],
            "voice_priority": item["priority"],
            "voice_duration": item["attachments"][-1].get("vmDuration"),
            "voice_transcription_status": item.get("vmTranscriptionStatus"),
            "file_uri": item["attachments"][-1].get("uri"),
            "file_type": item["attachments"][-1].get("type"),
            "file_format": item["attachments"][-1].get("contentType"),
            "file_name": item["attachments"][-1].get("fileName"),
            "voice_last_modified_time": item["lastModifiedTime"],

        }
        result.append(insert_parameters)

    pprint(result)
    return result


if __name__ == "__main__":
    input_data = open_test_payload("voice_ringcentral_payload.json")

    # # Array of objects format
    parsed_voice_list = parse_voice_to_list(input_data)
    write_voice_result_array(parsed_voice_list)
