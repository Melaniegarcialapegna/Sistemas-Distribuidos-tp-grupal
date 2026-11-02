from asyncio import IncompleteReadError
import os

from . import external_serializer, tlv_protocol

ROW_BATCH_SIZE = 1000 # add to config
USABLE_COLUMNS = ["name", "url", "date_modified", "sections", "image", "abstract", "in_language"] 

class MsgType:
    ACK = 1
    END_OF_RECORDS = 2
    ARTICLE_RECORD_BATCH = 3
    QUERY_RESULT_BATCH = 4
    QUERY_DONE = 5

class Query:
    Q1 , Q2 , Q3 , Q4 , Q5 = 1 , 2 , 3 , 4 , 5


def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))

def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)


def _recv_sized(socket, size):
    """
    Receives exactly 'num_bytes' bytes through the provided socket.
    If no bytes are read from the socket IncompleteReadError is raised
    """
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def _recv_empty(socket):
    return None

def _recv_article_record_batch(socket):
    batch_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    batch_data = []
    for i in range(batch_size):
        record = {}
        for column in USABLE_COLUMNS:
            column_size = external_serializer.deserialize_uint32(
                _recv_sized(socket, external_serializer.UINT32_SIZE)
            )
            column_value = external_serializer.deserialize_string(
                _recv_sized(socket, column_size)
            )
            record[column] = column_value
        batch_data.append(record)
    return batch_data

def _send_ack(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))

def _send_end_of_records(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.END_OF_RECORDS))

def _send_article_record_batch(socket, batch_data):
    parts = [
        external_serializer.serialize_uint32(MsgType.ARTICLE_RECORD_BATCH),
        external_serializer.serialize_uint32(len(batch_data)),
    ]
    for record in batch_data:
        for column in USABLE_COLUMNS:
            val = record[column]

            if not isinstance(val, str):
                val = str(val) if val is not None else ""

            value = external_serializer.serialize_string(val)
            parts.append(external_serializer.serialize_uint32(len(value)))
            parts.append(value)
    socket.sendall(b"".join(parts))

def _recv_query_result_batch(socket):
    query_id = external_serializer.deserialize_uint32(_recv_sized(socket, external_serializer.UINT32_SIZE))

    n_rows = external_serializer.deserialize_uint32(_recv_sized(socket, external_serializer.UINT32_SIZE))

    rows = []

    for _ in range(n_rows):
        size = external_serializer.deserialize_uint32(_recv_sized(socket, external_serializer.UINT32_SIZE))
        result_handler = RESULT_MSG_HANDLERS[query_id]
        result = result_handler(socket, size)
        rows.append(result)

    return (query_id, rows)

def _recv_query_done(socket):
    query_id = external_serializer.deserialize_uint32(_recv_sized(socket, external_serializer.UINT32_SIZE))
    return query_id

def _send_query_result_batch(socket, query_id, rows):
    parts = [
        external_serializer.serialize_uint32(MsgType.QUERY_RESULT_BATCH),
        external_serializer.serialize_uint32(query_id),
        external_serializer.serialize_uint32(len(rows)),
    ]
    for row in rows:
        data = tlv_protocol.serialize_tlv(row)
        parts.append(external_serializer.serialize_uint32(len(data)))
        parts.append(data)
    socket.sendall(b"".join(parts))

def _send_query_done(socket, query_id):
    socket.sendall(external_serializer.serialize_uint32(MsgType.QUERY_DONE) + external_serializer.serialize_uint32(query_id))

def receive_q1_result(socket, size):
    url, date = tlv_protocol.deserialize_tlv(_recv_sized(socket, size))
    row = (
        f" 🔗 URL      : {url}\n"
        f" 📅 Date     : {date}\n"
    )
    return row


def receive_q2_result(socket, size):
    name, url, img_bytes = tlv_protocol.deserialize_tlv(_recv_sized(socket, size))
    row = f"name: {name} - url: {url}"

    img_status = "No image found"

    # Por ahora se guardan para comprobar que funciona. 
    if img_bytes:
        safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '_', '-')).strip()
        filename = f"{safe_name}.jpg"
        
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "wb") as f:
            f.write(img_bytes)
            
        img_status = f"Saved in '{filepath}'"

    row = (
        f" 📖 Name     : {name}\n"
        f" 🔗 URL      : {url}\n"
        f" 📸 {img_status}\n"
    )
    return row

def receive_q3_result(socket, size):
    word = tlv_protocol.deserialize_tlv(_recv_sized(socket, size))
    row = (
        f" 📖 Word     : {word}\n"
    )
    return row
    

def receive_q4_result(socket, size):
    pass

def receive_q5_result(socket, size):
    pass


RECV_MSG_HANDLERS = {
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECORDS: _recv_empty,
    MsgType.ARTICLE_RECORD_BATCH: _recv_article_record_batch,
    MsgType.QUERY_RESULT_BATCH: _recv_query_result_batch,
    MsgType.QUERY_DONE: _recv_query_done,
}

SEND_MSG_HANDLERS = {
    MsgType.ACK: _send_ack,
    MsgType.END_OF_RECORDS: _send_end_of_records,
    MsgType.ARTICLE_RECORD_BATCH: _send_article_record_batch,
    MsgType.QUERY_RESULT_BATCH: _send_query_result_batch,
    MsgType.QUERY_DONE: _send_query_done,
}

RESULT_MSG_HANDLERS = {
    Query.Q1: receive_q1_result,
    Query.Q2: receive_q2_result,
    Query.Q3: receive_q3_result, 
    Query.Q4: receive_q4_result,
    Query.Q5: receive_q5_result
}
