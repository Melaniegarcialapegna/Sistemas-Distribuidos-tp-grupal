TYPE_REPRESENTATIONS = {
    "INT": 1,
    "STRING": 2,
    "BYTES": 3,
    "FLOAT": 4,
}

# Pensandolo bien, tener una clase Message para todos los mensajes no tiene mucho sentido, 
# porque cada mensaje tiene su propia estructura y no hay un mensaje que sea "padre" de todos los demas. 
# En tal caso cada mensaje deberia tener su propia clase, y que cada clase tenga su propio metodo serialize y deserialize. 
# De esta manera, cada mensaje va a ser responsable de serializar y deserializar sus propios datos, 
# y no vamos a tener que depender de una clase Message generica.
# El drawback de eso seria que medio que explota la cantidad de clases que tenemos y cada vez que agregamos un mensaje hay que crear una clase nueva.
# La dejo porque puede llegar a ser necesaria
class Message:
    def __init__(self, params):
        self.params = params

    # Creo que no lo vamos a usar, para serializar me parece mas sencillo usar la funcion directamente
    def serialize(self):
        return serialize_tlv(self.params)

    @staticmethod
    def from_bytes(data):
        return Message(deserialize_tlv(data))

def serialize_tlv(params):
    """
    Serlialize a list of ints and strings into a TLV format.

    :param params: A list of int and string objects to be serialized.
    :return: A bytes object representing the serialized TLV data.
    """
    serialized_data = bytearray()
    for item in params:
        if isinstance(item, int):
            serialized_data.extend(_serialize_int(item))
        elif isinstance(item, str):
            serialized_data.extend(_serialize_string(item))
        elif isinstance(item, bytes):
            serialized_data.extend(_serialize_bytes(item))
        elif isinstance(item, float):
            serialized_data.extend(_serialize_float(item))
        else:
            raise ValueError(f"Unsupported type: {type(item)}")
    return bytes(serialized_data)

def _serialize_string(value):
    """
    Serialize a string into a TLV format.

    :param value: A string to be serialized.
    :return: A bytes object representing the serialized TLV data for the string.
    """
    encoded_string = value.encode("utf-8")
    length = len(encoded_string)
    type_byte = TYPE_REPRESENTATIONS['STRING'].to_bytes(1, "big")
    return type_byte + length.to_bytes(4, "big") + encoded_string

def _serialize_int(value):
    """
    Serialize an integer into a TLV format.

    :param value: An integer to be serialized.
    :return: A bytes object representing the serialized TLV data for the integer.
    """
    type_byte = TYPE_REPRESENTATIONS['INT'].to_bytes(1, "big")
    return type_byte + value.to_bytes(4, "big")

def _serialize_bytes(value):
    """
    Serialize a bytes object into a TLV format.

    :param value: A bytes object to be serialized.
    :return: A bytes object representing the serialized TLV data for the bytes.
    """
    length = len(value)
    type_byte = TYPE_REPRESENTATIONS['BYTES'].to_bytes(1, "big")
    return type_byte + length.to_bytes(4, "big") + value

def _serialize_float(value):
    """
    Serialize a float into a TLV format.

    :param value: A float to be serialized.
    :return: A bytes object representing the serialized TLV data for the float.
    """
    # No veo como serializar un float en TLV, porque no hay un formato estandar para eso.
    # Existe el modulo struct que permite serializar floats, pero no se si lo podemos usar.
    # Como es para la Q5 recien se puede posponer por el moemento.
    raise NotImplementedError("Float serialization is not implemented yet.")

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
        if type_byte == TYPE_REPRESENTATIONS['INT']:
            value, consumed_bytes = _deserialize_int(data[index + 1:])
            deserialized_data.append(value)
        elif type_byte == TYPE_REPRESENTATIONS['STRING']:
            value, consumed_bytes = _deserialize_string(data[index + 1:])
            deserialized_data.append(value)
        elif type_byte == TYPE_REPRESENTATIONS['BYTES']:
            value, consumed_bytes = _deserialize_bytes(data[index + 1:])
            deserialized_data.append(value)
        elif type_byte == TYPE_REPRESENTATIONS['FLOAT']:
            value, consumed_bytes = _deserialize_float(data[index + 1:])
            deserialized_data.append(value)
        else:
            raise ValueError(f"Unsupported type byte: {type_byte}")
        index += consumed_bytes + 1 # me muevo al siguiente tipo, sumo 1 para el byte de tipo
    return deserialized_data

def _deserialize_string(data):
    """
    Deserialize a TLV formatted string.

    :param data: A bytes object representing the serialized TLV data for the string, excluding the type byte.
    :return: The deserialized string.
    """
    length = int.from_bytes(data[:4], "big")
    return data[4:4 + length].decode("utf-8"), length + 4 # Return the amount of bytes consumed (length of string + 4 bytes for length field)

def _deserialize_int(data):
    """
    Deserialize a TLV formatted integer.

    :param data: A bytes object representing the serialized TLV data for the integer, excluding the type byte.
    :return: The deserialized integer.
    """
    return int.from_bytes(data[:4], "big"), 4 # Return the amount of bytes consumed (4 bytes for the integer)

def _deserialize_bytes(data):
    """
    Deserialize a TLV formatted bytes object.

    :param data: A bytes object representing the serialized TLV data for the bytes, excluding the type byte.
    :return: The deserialized bytes object.
    """
    length = int.from_bytes(data[:4], "big")
    return data[4:4 + length], length + 4 # Return the amount of bytes consumed (length of bytes + 4 bytes for length field)

def _deserialize_float(data):
    """
    Deserialize a TLV formatted float.

    :param data: A bytes object representing the serialized TLV data for the float, excluding the type byte.
    :return: The deserialized float.
    """
    # Idem que para serialize_float, no veo como deserializar un float en TLV, porque no hay un formato estandar para eso.
    raise NotImplementedError("Float deserialization is not implemented yet.")