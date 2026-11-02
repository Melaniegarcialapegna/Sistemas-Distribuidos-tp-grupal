TYPE_REPRESENTATIONS = {
    "INT": 1,
    "STRING": 2,
    "BYTES": 3,
    "FLOAT": 4,
    "LIST": 5,
}


# The length of the length field (not the data) in bytes for all types.
LENGTH_BYTES = 4


def serialize_string(value):
    """
    Serialize a string into a TLV format.

    :param value: A string to be serialized.
    :return: A bytes object representing the serialized TLV data for the string.
    """
    encoded_string = value.encode("utf-8")
    length = len(encoded_string)
    type_byte = TYPE_REPRESENTATIONS['STRING'].to_bytes(1, "big")
    return type_byte + length.to_bytes(LENGTH_BYTES, "big") + encoded_string

def serialize_int(value):
    """
    Serialize an integer into a TLV format.

    :param value: An integer to be serialized.
    :return: A bytes object representing the serialized TLV data for the integer.
    """
    type_byte = TYPE_REPRESENTATIONS['INT'].to_bytes(1, "big")
    length_bytes_quantity = (value.bit_length() + 7) // 8 or 1  # cantidad minima de bytes para representar el int
    length_bytes = length_bytes_quantity.to_bytes(LENGTH_BYTES, "big")
    return type_byte + length_bytes + value.to_bytes(length_bytes_quantity, "big")

def serialize_bytes(value):
    """
    Serialize a bytes object into a TLV format.

    :param value: A bytes object to be serialized.
    :return: A bytes object representing the serialized TLV data for the bytes.
    """
    length = len(value)
    type_byte = TYPE_REPRESENTATIONS['BYTES'].to_bytes(1, "big")
    return type_byte + length.to_bytes(LENGTH_BYTES, "big") + value

def serialize_float(value):
    """
    Serialize a float into a TLV format.

    :param value: A float to be serialized.
    :return: A bytes object representing the serialized TLV data for the float.
    """
    # No veo como serializar un float en TLV, porque no hay un formato estandar para eso.
    # Existe el modulo struct que permite serializar floats, pero no se si lo podemos usar.
    # Como es para la Q5 recien se puede posponer por el moemento.
    raise NotImplementedError("Float serialization is not implemented yet.")

def serialize_list(list):
    """
    Serialize a list into a TLV format.

    :param list: A list of int and string objects to be serialized.
    :return: A bytes object representing the serialized TLV data for the list.
    """
    serialized_data = serialize_tlv(list)
    length = len(serialized_data)
    type_byte = TYPE_REPRESENTATIONS['LIST'].to_bytes(1, "big")
    return type_byte + length.to_bytes(LENGTH_BYTES, "big") + serialized_data

SERIALIZATION_METHODS = {
    int: serialize_int,
    str: serialize_string,
    bytes: serialize_bytes,
    list: serialize_list,
    float: serialize_float,
}

def serialize_tlv(params):
    """
    Serlialize a list of ints and strings into a TLV format.

    :param params: A list of int and string objects to be serialized.
    :return: A bytes object representing the serialized TLV data.
    """
    serialized_data = bytearray()
    for item in params:
        if type(item) not in SERIALIZATION_METHODS:
            raise ValueError(f"Unsupported type: {type(item)}")
        serialized_data.extend(SERIALIZATION_METHODS[type(item)](item))
    return bytes(serialized_data)


def deserialize_string(data):
    """
    Deserialize a TLV formatted string.

    :param data: A bytes object representing the serialized TLV data for the string, excluding the type byte.
    :return: The deserialized string.
    """
    return data.decode("utf-8")

def deserialize_int(data):
    """
    Deserialize a TLV formatted integer.

    :param data: A bytes object representing the serialized TLV data for the integer, excluding the type byte.
    :return: The deserialized integer.
    """
    return int.from_bytes(data, "big")

def deserialize_float(data):
    """
    Deserialize a TLV formatted float.

    :param data: A bytes object representing the serialized TLV data for the float, excluding the type byte.
    :return: The deserialized float.
    """
    # Idem que para serialize_float, no veo como deserializar un float en TLV, porque no hay un formato estandar para eso.
    raise NotImplementedError("Float deserialization is not implemented yet.")

DESERIALIZATION_METHODS = {
    TYPE_REPRESENTATIONS['INT']: deserialize_int,
    TYPE_REPRESENTATIONS['STRING']: deserialize_string,
    TYPE_REPRESENTATIONS['BYTES']: lambda x: x,  # bytes ya vienen encodeados
    TYPE_REPRESENTATIONS['FLOAT']: deserialize_float,
    TYPE_REPRESENTATIONS['LIST']: lambda x: deserialize_tlv(x),
}
def deserialize_tlv(data):
    """
    Deserialize a TLV formatted bytes object into a list of ints and strings.

    :param data: A bytes object representing the serialized TLV data.
    :return: A list of deserialized int and string objects.
    """
    deserialized_data = []
    index = 0
    while index < len(data):
        type_byte = data[index]
        if type_byte not in DESERIALIZATION_METHODS:
            raise ValueError(f"Unknown type byte: {type_byte}")
        index += 1
        length = int.from_bytes(data[index:index + LENGTH_BYTES], "big")
        index += LENGTH_BYTES
        data_segment = data[index:index + length]
        value = DESERIALIZATION_METHODS[type_byte](data_segment)
        deserialized_data.append(value)
        index += length
    return deserialized_data