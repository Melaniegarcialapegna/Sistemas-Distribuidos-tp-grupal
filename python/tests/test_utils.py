import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from common.utils import (
    fnv1a_32,
    FNV32_OFFSET,
    get_env,
    get_env_list,
    get_service_info,
    get_heartbeat_config,
    get_control_info,
    get_worker_info,
    get_watchdog_config,
)


class TestUtils(unittest.TestCase):
    def test_fnv1a_32_deterministic(self):
        self.assertEqual(fnv1a_32(b"hello"), fnv1a_32(b"hello"))
        self.assertEqual(fnv1a_32(b"world"), fnv1a_32(b"world"))

    def test_fnv1a_32_different_inputs(self):
        self.assertNotEqual(fnv1a_32(b"apple"), fnv1a_32(b"banana"))

    def test_fnv1a_32_empty_bytes(self):
        self.assertEqual(fnv1a_32(b""), FNV32_OFFSET)

    def test_fnv1a_32_range(self):
        for sample in [b"a", b"long string with many characters", b"\x00\xff"]:
            h = fnv1a_32(sample)
            self.assertGreaterEqual(h, 0)
            self.assertLessEqual(h, 0xFFFFFFFF)

    def test_get_env_reads_and_casts_required_var(self):
        with patch.dict(os.environ, {"TEST_INT": "42", "TEST_FLOAT": "1.5", "TEST_STR": "hello"}, clear=False):
            self.assertEqual(get_env("TEST_INT", int), 42)
            self.assertEqual(get_env("TEST_FLOAT", float), 1.5)
            self.assertEqual(get_env("TEST_STR"), "hello")

    def test_get_env_missing_raises_key_error_without_default(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(KeyError):
                get_env("MISSING_VAR")

    def test_get_env_returns_default_when_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(get_env("MISSING_VAR", default=None))
            self.assertEqual(get_env("MISSING_INT", int, default=10), 10)

    def test_get_env_list_parses_and_strips(self):
        with patch.dict(os.environ, {"TEST_LIST": " a , b, , c "}, clear=False):
            self.assertEqual(get_env_list("TEST_LIST"), ["a", "b", "c"])

    def test_get_env_list_missing_raises_or_uses_default(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(KeyError):
                get_env_list("MISSING_LIST")
            self.assertEqual(get_env_list("MISSING_LIST", default=[]), [])

    def test_get_service_info(self):
        env = {"RABBITMQ_HOST": "rabbitmq", "REPLICA_ID": "2", "SERVICE_NAME": "word_counter_2"}
        with patch.dict(os.environ, env, clear=True):
            info_str = get_service_info()
            self.assertEqual(info_str.rabbitmq_host, "rabbitmq")
            self.assertEqual(info_str.replica_id, "2")
            self.assertEqual(info_str.service_name, "word_counter_2")

            info_int = get_service_info(replica_id_cast=int, require_service_name=True)
            self.assertEqual(info_int.replica_id, 2)

    def test_get_heartbeat_config_optional_and_required(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(get_heartbeat_config(required=False))
            with self.assertRaises(KeyError):
                get_heartbeat_config(required=True)

        env = {
            "HEARTBEAT_EXCHANGE_NAME": "hb_ex",
            "HEARTBEAT_INTERVAL": "1.5",
            "SERVICE_NAME": "tokenizer_0",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = get_heartbeat_config(required=False, require_service_name=True)
            self.assertEqual(cfg.exchange_name, "hb_ex")
            self.assertEqual(cfg.interval, 1.5)
            self.assertEqual(cfg.service_name, "tokenizer_0")

    def test_get_control_info(self):
        env = {
            "CONTROL_EXCHANGE_NAME": "ctrl_ex",
            "CONTROL_ROUTING_KEY": "rk_0",
            "PREVIOUS_STAGE_REPLICAS": "3",
            "NEXT_STAGE_REPLICAS": "2",
        }
        with patch.dict(os.environ, env, clear=True):
            ctrl = get_control_info()
            self.assertEqual(ctrl.control_exchange_name, "ctrl_ex")
            self.assertEqual(ctrl.control_routing_key, "rk_0")
            self.assertEqual(ctrl.previous_stage_replicas, 3)
            self.assertEqual(ctrl.next_stage_replicas, 2)

    def test_get_worker_info(self):
        env = {
            "RABBITMQ_HOST": "rabbitmq",
            "REPLICA_ID": "0",
            "INPUT_QUEUE_NAME": "in_q",
            "BATCH_SIZE": "50",
            "CONTROL_EXCHANGE_NAME": "ctrl_ex",
        }
        with patch.dict(os.environ, env, clear=True):
            info = get_worker_info(batch_size_default=10, require_input_queue=True)
            self.assertEqual(info.service.rabbitmq_host, "rabbitmq")
            self.assertEqual(info.service.replica_id, "0")
            self.assertEqual(info.control.control_exchange_name, "ctrl_ex")
            self.assertEqual(info.batch_size, 50)
            self.assertEqual(info.input_queue_name, "in_q")

        with patch.dict(os.environ, {"RABBITMQ_HOST": "rabbitmq", "REPLICA_ID": "1"}, clear=True):
            with self.assertRaises(KeyError):
                get_worker_info(require_input_queue=True)
            info_opt = get_worker_info(batch_size_default=100, require_input_queue=False)
            self.assertEqual(info_opt.batch_size, 100)
            self.assertIsNone(info_opt.input_queue_name)

    def test_get_watchdog_config_fails_if_any_var_missing(self):
        full_env = {
            "RABBITMQ_HOST": "localhost",
            "HEARTBEAT_INTERVAL": "1.0",
            "HEARTBEAT_TIMEOUT": "3.0",
            "INITIAL_GRACE_PERIOD": "5.0",
            "REVIVE_GRACE_PERIOD": "5.0",
            "WATCHLIST": "worker_0,worker_1",
            "REPLICA_ID": "1",
            "TOTAL_WATCHDOGS": "3",
            "ELECTION_TIMEOUT": "1.5",
            "HEARTBEAT_EXCHANGE_NAME": "hb_ex",
            "WATCHDOG_EXCHANGE_NAME": "wd_ex",
        }
        with patch.dict(os.environ, full_env, clear=True):
            cfg = get_watchdog_config()
            self.assertEqual(cfg.service.rabbitmq_host, "localhost")
            self.assertEqual(cfg.service.replica_id, 1)
            self.assertEqual(cfg.heartbeat.exchange_name, "hb_ex")
            self.assertEqual(cfg.watchlist, ["worker_0", "worker_1"])
            self.assertEqual(cfg.total_watchdogs, 3)

        for missing_key in full_env:
            partial_env = {k: v for k, v in full_env.items() if k != missing_key}
            with patch.dict(os.environ, partial_env, clear=True):
                with self.assertRaises(KeyError):
                    get_watchdog_config()


if __name__ == "__main__":
    unittest.main()

