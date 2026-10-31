from abc import ABC, abstractmethod

from src.common.message_protocol.tlv_protocol import deserialize_tlv, serialize_tlv

class MessageType(ABC):
    """
    Clase que representa los tipos de mensajes utilizados en el protocolo de comunicación.
    """

    def __init__(self, payload, from_client_id):
        """
        Inicializa una instancia de MessageType.

        :param payload: Lista de elementos que representan la carga útil del mensaje.
        :param from_client_id: Identificador del cliente al que pertenece el mensaje.
        """
        self.payload = payload
        self.from_client_id = from_client_id

    @abstractmethod
    def visit(self, visitor):
        """
        Permite que un visitante procese o serialize el mensaje.

        :param visitor: El visitante que procesará el mensaje.
        """
        pass

class EOFMessageType(MessageType):
    """
    Clase que representa un mensaje de fin de flujo (EOF).
    """

    def visit(self, visitor):
        return visitor.visit_eof(self)

class DataMessageType(MessageType):
    """
    Clase que representa un mensaje de datos.
    """

    def visit(self, visitor):
        return visitor.visit_data(self)

class BatchMessageType(MessageType):
    """
    Clase que representa un mensaje de lote (batch).
    """

    def visit(self, visitor):
        return visitor.visit_batch(self)

class ControlMessageType(MessageType):
    """
    Clase que representa un mensaje de control.
    """

    def visit(self, visitor):
        return visitor.visit_control(self)