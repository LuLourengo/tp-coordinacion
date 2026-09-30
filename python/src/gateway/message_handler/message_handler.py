import uuid
from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().hex
        self.data_messages_sent = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.data_messages_sent += 1
        return message_protocol.internal.serialize(
            [self.client_id, "DATA", str(fruit), int(amount)]
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(
            [self.client_id, "EOF", self.data_messages_sent]
        )

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) >= 2 and str(fields[0]) == str(self.client_id):
            formatted_top = []
            for item in fields[1]:
                formatted_top.append((str(item[0]), int(item[1])))
            return formatted_top
        return None