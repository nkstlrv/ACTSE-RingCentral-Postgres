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

    with PostgresHandler(DB_CONFIG) as db_handler:
        create_tabel_query = """
            CREATE TABLE IF NOT EXISTS ringcentral_voice_mail (
            id bigserial PRIMARY KEY,
            vm_id VARCHAR(255) UNIQUE NOT NULL,
            to_name VARCHAR(255),
            to_location VARCHAR(255),
            from_name VARCHAR(255),
            from_phone_nuber VARCHAR(255),
            from_location VARCHAR(255),
            contact_availability VARCHAR(255),
            vm_direction VARCHAR(255),
            vm_type VARCHAR(255),
            vm_delivery_status VARCHAR(255),
            vm_read_status VARCHAR(255),
            vm_priority VARCHAR(255),
            vm_duration VARCHAR(255),
            vm_time VARCHAR(255),
            vm_time_modified VARCHAR(255),
            vm_transcription_status VARCHAR(255),
            vm_file_uri VARCHAR(255),
            vm_file_type VARCHAR(255),
            vm_file_format VARCHAR(255),
            vm_file_name VARCHAR(255)
        );

        """
        db_handler.execute(create_tabel_query)

        for item in voice:
            insert_parameters = {
                "vm_id": item["id"],
                "to_name": item["to"][-1].get("name"),
                "to_location": item["to"][-1].get("location"),
                "from_name": item["from"].get("name"),
                "from_phone_nuber": item["from"].get("phoneNumber"),
                "from_location": item["from"].get("location"),
                "contact_availability": item["availability"],
                "vm_direction": item["direction"],
                "vm_type": item["type"],
                "vm_delivery_status": item["messageStatus"],
                "vm_read_status": item["readStatus"],
                "vm_priority": item["priority"],
                "vm_duration": item["attachments"][-1].get("vmDuration"),
                "vm_time": item["creationTime"],
                "vm_time_modified": item["lastModifiedTime"],
                "vm_transcription_status": item.get("vmTranscriptionStatus"),
                "vm_file_uri": item["attachments"][-1].get("uri"),
                "vm_file_type": item["attachments"][-1].get("type"),
                "vm_file_format": item["attachments"][-1].get("contentType"),
                "vm_file_name": item["attachments"][-1].get("fileName"),


            }
            result.append(insert_parameters)

            insert_query = """
                                        INSERT INTO ringcentral_voice_mail 
                                        (vm_id,
                                        to_name,
                                        to_location,
                                        from_name,
                                        from_phone_nuber,
                                        from_location,
                                        contact_availability,
                                        vm_direction,
                                        vm_type,
                                        vm_delivery_status,
                                        vm_read_status,
                                        vm_priority,
                                        vm_duration,
                                        vm_time,
                                        vm_time_modified,
                                        vm_transcription_status,
                                        vm_file_uri,
                                        vm_file_type,
                                        vm_file_format,
                                        vm_file_name
                                        )
                                        VALUES (
                                        %(vm_id)s, 
                                        %(to_name)s, 
                                        %(to_location)s, 
                                        %(from_name)s, 
                                        %(from_phone_nuber)s, 
                                        %(from_location)s, 
                                        %(contact_availability)s, 
                                        %(vm_direction)s, 
                                        %(vm_type)s, 
                                        %(vm_delivery_status)s, 
                                        %(vm_read_status)s, 
                                        %(vm_priority)s, 
                                        %(vm_duration)s, 
                                        %(vm_time)s, 
                                        %(vm_time_modified)s, 
                                        %(vm_transcription_status)s, 
                                        %(vm_file_uri)s, 
                                        %(vm_file_type)s, 
                                        %(vm_file_format)s, 
                                        %(vm_file_name)s
                                        );
                                    """

            db_handler.execute(insert_query, insert_parameters)
            print(f"INSERTING SMS {insert_parameters['vm_id']}")

    pprint(result)
    return result


if __name__ == "__main__":
    input_data = open_test_payload("vm_payload.json")

    # # Array of objects format
    parsed_vm_list = parse_vm_to_list(input_data)
    write_vm_result_array(parsed_vm_list)
