import unittest
from unittest.mock import patch, mock_open
from datetime import datetime
from typing import List
from functions.Rows import Rows
from functions.ExecutionResult import ExecutionResult
from functions.RecoverCriteria import RecoverCriteria
from core.FailureRecovery import FailureRecovery

class TestExecutionResult(unittest.TestCase):
    def test_init(self):
        transaction_id = 1
        timestamp = datetime.now()
        message = "Test Message"
        data = Rows([1, 2, 3])  # Here, the data uses Rows (initialized)
        query = "SELECT * FROM table"

        result = ExecutionResult(transaction_id, timestamp, message, data, query)

        self.assertEqual(result.transaction_id, transaction_id)
        self.assertEqual(result.timestamp, timestamp)
        self.assertEqual(result.message, message)
        self.assertEqual(result.data, data)
        self.assertEqual(result.query, query)

    def test_init_intdata(self):
        transaction_id = 2
        timestamp = datetime.now()
        message = "Another Test"
        data = 42
        query = "UPDATE table SET column=42"

        result = ExecutionResult(transaction_id, timestamp, message, data, query)

        self.assertEqual(result.transaction_id, transaction_id)
        self.assertEqual(result.timestamp, timestamp)
        self.assertEqual(result.message, message)
        self.assertEqual(result.data, data)
        self.assertEqual(result.query, query)

    def test_init_nodata(self):
        transaction_id = 3
        timestamp = datetime.now()
        message = "None Data Test"
        data = None
        query = "DELETE FROM table WHERE id=3"

        result = ExecutionResult(transaction_id, timestamp, message, data, query)

        self.assertEqual(result.transaction_id, transaction_id)
        self.assertEqual(result.timestamp, timestamp)
        self.assertEqual(result.message, message)
        self.assertIsNone(result.data)
        self.assertEqual(result.query, query)


class TestRows(unittest.TestCase):
    def test_init(self):
        data = [1, 2, 3]
        rows = Rows(data)

        self.assertEqual(rows.data, data)
        self.assertEqual(rows.rows_count, len(data))

    def test_init_nodata(self):
        data = []
        rows = Rows(data)

        self.assertEqual(rows.data, data)
        self.assertEqual(rows.rows_count, 0)

    def test_string(self):
        data = ["row1", "row2", "row3"]
        rows = Rows(data)

        self.assertEqual(rows.data, data)
        self.assertEqual(rows.rows_count, len(data))


class TestRecoverCriteria(unittest.TestCase):
    def test_init_values(self):
        timestamp = datetime.now()
        transaction_id = 100

        criteria = RecoverCriteria(timestamp=timestamp, transaction_id=transaction_id)

        self.assertEqual(criteria.timestamp, timestamp)
        self.assertEqual(criteria.transaction_id, transaction_id)

    def test_init_novalues(self):
        criteria = RecoverCriteria()

        self.assertIsNone(criteria.timestamp)
        self.assertIsNone(criteria.transaction_id)

class TestFailureRecovery(unittest.TestCase):
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=True)
    @patch("json.dump")
    def test_write_log(self, mock_json_dump, mock_exists, mock_file):
        recovery_manager = FailureRecovery("test_log.json")

        # to make an empy mock file
        mock_file().read.return_value = ""
        recovery_manager._load_logs()
        self.assertEqual(recovery_manager.write_ahead_log, [])

        recovery_manager.write_log(
            ExecutionResult(
                transaction_id=1,
                timestamp=datetime.now(),
                message="start",
                data=None,
                query=""
            )
        )
        self.assertEqual(
            recovery_manager.write_ahead_log,
            [[1, "start"]]
        )

        recovery_manager.write_log(
            ExecutionResult(
                transaction_id=1,
                timestamp=datetime.now(),
                message="write",
                data=100,
                query="UPDATE X SET value=100"
            )
        )
        self.assertEqual(
            recovery_manager.write_ahead_log,
            [[1, "start"], [1, "X", "dummies_oldies_valies", 100]]
        )

        recovery_manager.write_log(
            ExecutionResult(
                transaction_id=1,
                timestamp=datetime.now(),
                message="commit",
                data=None,
                query="COMMIT"
            )
        )
        self.assertEqual(
            recovery_manager.write_ahead_log,
            [[1, "start"], [1, "X", "dummies_oldies_valies", 100], [1, "commit"]]
        )

    @patch("builtins.print")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=True)
    def test_display_logs(self, mock_exists, mock_file, mock_print):
        recovery_manager = FailureRecovery("test_log.json")

        recovery_manager.write_ahead_log = [
            [1, "start"],
            [1, "X", "dummies_oldies_valies", 100],
            [1, "commit"]
        ]

        recovery_manager.display_logs()

        mock_print.assert_any_call("\nIn Memory (?) Write Ahead Log:")
        mock_print.assert_any_call([1, "start"])
        mock_print.assert_any_call([1, "X", "dummies_oldies_valies", 100])
        mock_print.assert_any_call([1, "commit"])

if __name__ == "__main__":
    unittest.main()
