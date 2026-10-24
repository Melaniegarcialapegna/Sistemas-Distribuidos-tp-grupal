from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Union


class MiddlewareError(Exception):
    """Base exception for middleware errors."""
    pass


class MessageMiddlewareMessageError(MiddlewareError):
    """Raised when an unrecoverable message or broker processing error occurs."""
    pass


class MessageMiddlewareDisconnectedError(MiddlewareError):
    """Raised when connection to the middleware is lost."""
    pass


class MessageMiddlewareCloseError(MiddlewareError):
    """Raised when closing the connection fails."""
    pass


class MessageMiddlewareDeleteError(MiddlewareError):
    """Raised when deleting a queue or exchange fails."""
    pass


# Aliases for consistent naming
MiddlewareMessageError = MessageMiddlewareMessageError
MiddlewareDisconnectedError = MessageMiddlewareDisconnectedError
MiddlewareCloseError = MessageMiddlewareCloseError
MiddlewareDeleteError = MessageMiddlewareDeleteError


class MiddlewareHandler(ABC):
    """
    Interfaz abstracta del middleware de mensajería (MOM).
    Define las operaciones que necesita cualquier proceso para comunicarse
    (producción, suscripción y consumo de mensajes) abstrayendo la tecnología subyacente.
    """

    @abstractmethod
    def subscribe_to_queue(
        self,
        queue_name: str,
        callback: Callable
    ) -> None:
        """
        Suscribe el middleware a una cola específica asociándole su propia función de callback.
        Permite suscribirse a múltiples colas invocando este método sucesivamente con
        un callback específico para cada una.

        :param queue_name: Nombre de la cola a escuchar.
        :param callback: Función invocada al recibir un mensaje de esta cola: callback(body, ack, nack).
        """
        pass

    @abstractmethod
    def subscribe_to_exchange(
        self,
        exchange_name: str,
        routing_keys: Optional[Union[str, List[str]]] = None,
        callback: Callable = None,
        **kwargs
    ) -> str:
        """
        Suscribe el middleware a un exchange con una o varias routing keys, asociándole
        su propia función de callback a la cola creada para el exchange.

        :param exchange_name: Nombre del exchange a escuchar.
        :param routing_keys: Clave o lista de claves de enrutamiento para vincular.
        :param callback: Función invocada al recibir un mensaje de este exchange: callback(body, ack, nack).
        :return: Nombre de la cola vinculada al exchange.
        """
        pass

    @abstractmethod
    def start_consuming(self) -> None:
        """
        Comienza a escuchar y consumir mensajes de todas las colas y exchanges
        a los que se haya suscripto el handler, despachando cada mensaje a la función
        de callback asociada a su respectiva cola.

        :raises MiddlewareDisconnectedError: Si se pierde la conexión con el broker.
        :raises MiddlewareMessageError: Si ocurre un error interno durante el consumo.
        """
        pass

    @abstractmethod
    def stop_consuming(self) -> None:
        """
        Detiene la escucha y consumo de mensajes. Si no se estaba consumiendo,
        no tiene efecto.

        :raises MiddlewareDisconnectedError: Si se pierde la conexión.
        :raises MiddlewareMessageError: Si ocurre un error interno al detener.
        """
        pass

    @abstractmethod
    def subscribe_to_queue(
        queue_name: str,
        callback: Callable
    ) -> None:
        """
        Suscribe el middleware a una cola específica asociándole su propia función de callback.
        Permite suscribirse a múltiples colas invocando este método sucesivamente con
        un callback específico para cada una.

        :param queue_name: Nombre de la cola a escuchar.
        :param callback: Función invocada al recibir un mensaje de esta cola: callback(body, ack, nack).
        """
        pass

    def subscribe_to_exchange(
        self,
        exchange_name: str,
        routing_keys: Optional[Union[str, List[str]]] = None,
        callback: Callable = None,
        **kwargs
    ) -> str:
        """
        Suscribe el middleware a un exchange con una o varias routing keys, asociándole
        su propia función de callback a la cola creada para el exchange.

        :param exchange_name: Nombre del exchange a escuchar.
        :param routing_keys: Clave o lista de claves de enrutamiento para vincular.
        :param callback: Función invocada al recibir un mensaje de este exchange: callback(body, ack, nack).
        :return: Nombre de la cola vinculada al exchange.
        """
        pass

    @abstractmethod
    def send(
        self,
        destination: str,
        payload: Union[bytes, str],
        routing_key: Optional[str] = None
    ) -> None:
        """
        Envía un mensaje a una cola o exchange.
        - Si routing_key es provisto (o destination es un exchange conocido),
          se publica al exchange destination con la routing_key especificada.
        - Si routing_key es None y destination es una cola, se publica directamente
          a dicha cola.

        :param destination: Nombre de la cola o exchange destino.
        :param payload: Contenido del mensaje (bytes o string).
        :param routing_key: Clave de enrutamiento (opcional).
        :raises MiddlewareDisconnectedError: Si se pierde la conexión.
        :raises MiddlewareMessageError: Si ocurre un error interno.
        """
        pass

    @abstractmethod
    def broadcast(
        self,
        exchange: str,
        payload: Union[bytes, str]
    ) -> None:
        """
        Envía un mensaje a todos los suscriptores del exchange especificado
        (típicamente fanout o direct sin clave específica).

        :param exchange: Nombre del exchange al que enviar en broadcast.
        :param payload: Contenido del mensaje (bytes o string).
        :raises MiddlewareDisconnectedError: Si se pierde la conexión.
        :raises MiddlewareMessageError: Si ocurre un error interno.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Cierra canales y conexiones liberando los recursos de red y broker.

        :raises MiddlewareCloseError: Si ocurre un error cerrando la conexión.
        """
        pass
