import json
from pprint import pprint
import pretty_errors
from db.db_handler import PostgresHandler
from calls_records.main_records import DB_CONFIG, open_test_payload


def write_sms_result_array(payload: list) -> None:
    with open("sms_records_result_list.json", "w") as fw:
        json.dump(payload, fw)
        print("SMS JSON (Array) created")


def parse_sms_to_list(payload: dict) -> list:
    sms: list = payload["value"]["ring_central"]["records"]
    result = list()

    with PostgresHandler(DB_CONFIG) as db_handler:
        # create_tabel_query = """
        #     CREATE TABLE IF NOT EXISTS ringcentral_sms (
        #     id bigserial PRIMARY KEY,
        #     sms_id VARCHAR(255) UNIQUE NOT NULL,
        #     sms_message TEXT,
        #     to_name VARCHAR(255),
        #     to_location VARCHAR(255),
        #     from_name VARCHAR(255),
        #     from_phone_nuber VARCHAR(255),
        #     from_location VARCHAR(255),
        #     contact_availability VARCHAR(255),
        #     sms_message_type VARCHAR(255),
        #     sms_delivery_status VARCHAR(255),
        #     sms_direction VARCHAR(255),
        #     sms_read_status VARCHAR(255),
        #     sms_priority VARCHAR(255),
        #     sms_time VARCHAR(255),
        #     sms_time_modified VARCHAR(255),
        #     conversation_id VARCHAR(255),
        #     conversation_uri VARCHAR(255)
        # );
        #
        # """
        # db_handler.execute(create_tabel_query)

        for item in sms:
            insert_parameters = {
                "sms_id": item["id"],
                "sms_message": item["subject"],
                "to_name": item["to"][-1].get("name"),
                "to_location": item["to"][-1].get("location"),
                "from_name": item["from"].get("name"),
                "from_phone_nuber": item["from"].get("phoneNumber"),
                "from_location": item["from"].get("location"),
                "contact_availability": item["availability"],
                "sms_message_type": item["type"],
                "sms_delivery_status": item["messageStatus"],
                "sms_direction": item["direction"],
                "sms_read_status": item["readStatus"],
                "sms_priority": item["priority"],
                "sms_time": item["creationTime"],
                "sms_time_modified": item["lastModifiedTime"],
                "conversation_id": item["conversationId"],
                "conversation_uri": item["conversation"].get("uri")

            }

            result.append(insert_parameters)

            insert_query = """
                            INSERT INTO ringcentral_sms 
                            (sms_id,
                            sms_message,
                            to_name,
                            to_location,
                            from_name,
                            from_phone_nuber,
                            from_location,
                            contact_availability,
                            sms_message_type,
                            sms_delivery_status,
                            sms_direction,
                            sms_read_status,
                            sms_priority,
                            sms_time,
                            sms_time_modified,
                            conversation_id,
                            conversation_uri

                            )
                            VALUES (
                            %(sms_id)s, 
                            %(sms_message)s, 
                            %(to_name)s, 
                            %(to_location)s, 
                            %(from_name)s, 
                            %(from_phone_nuber)s, 
                            %(from_location)s, 
                            %(contact_availability)s, 
                            %(sms_message_type)s, 
                            %(sms_delivery_status)s, 
                            %(sms_direction)s, 
                            %(sms_read_status)s, 
                            %(sms_priority)s, 
                            %(sms_time)s, 
                            %(sms_time_modified)s, 
                            %(conversation_id)s, 
                            %(conversation_uri)s
                            );
                        """

            db_handler.execute(insert_query, insert_parameters)
            print(f"INSERTING SMS {insert_parameters['sms_id']}")

    pprint(result)
    return result


if __name__ == "__main__":
    input_data = open_test_payload("sms_ringcentral_payload.json")

    # # Array of objects format
    parsed_sms_list = parse_sms_to_list(input_data)
    write_sms_result_array(parsed_sms_list)
