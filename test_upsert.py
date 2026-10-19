import unittest
import sqlite3
import time

UPSERT_QUERY = """
INSERT INTO scans (id, version, payload, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    version = excluded.version,
    payload = excluded.payload,
    updated_at = excluded.updated_at
WHERE excluded.version >= scans.version;
"""

class TestSQLiteUpsert(unittest.TestCase):
    def setUp(self):
        """Runs before every test: creates a fresh in-memory database."""
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("""
            CREATE TABLE scans (
                id TEXT PRIMARY KEY,
                version INTEGER,
                payload TEXT,
                updated_at REAL
            );
        """)
        self.conn.commit()

    def tearDown(self):
        """Runs after every test: cleanly closes the connection."""
        self.conn.close()

    def get_record(self, record_id: str):
        self.cursor.execute("SELECT id, version, payload, updated_at FROM scans WHERE id = ?", (record_id,))
        return self.cursor.fetchone()

    def test_fresh_insert(self):
        """Verify that a brand new record inserts successfully."""
        now = time.time()
        self.cursor.execute(UPSERT_QUERY, ("host-1", 2, '{"port": 443, "status": "OPEN"}', now))
        self.conn.commit()

        row = self.get_record("host-1")
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "host-1")
        self.assertEqual(row[1], 2)
        self.assertEqual(row[2], '{"port": 443, "status": "OPEN"}')

    def test_stale_write_rejected(self):
        """Verify that a write with an older version number is rejected."""
        t1 = 100.0
        t2 = 50.0  # Even with an earlier or later timestamp, version is the authority

        # 1. Host already has version 2 stored
        self.cursor.execute(UPSERT_QUERY, ("host-1", 2, '{"port": 443, "status": "OPEN"}', t1))
        self.conn.commit()

        # 2. Out-of-order / stale packet arrives with version 1
        self.cursor.execute(UPSERT_QUERY, ("host-1", 1, '{"port": 443, "status": "CLOSED"}', t2))
        self.conn.commit()

        # 3. Assert version 1 was SILENTLY DROPPED: state must still be version 2
        row = self.get_record("host-1")
        self.assertEqual(row[1], 2, "Stale version 1 overwrote version 2!")
        self.assertEqual(row[2], '{"port": 443, "status": "OPEN"}', "Stale payload overwrote fresh payload!")

    def test_newer_version_updates(self):
        """Verify that a write with a higher version updates the record."""
        t1 = 100.0
        t2 = 200.0

        # 1. Seed with version 2
        self.cursor.execute(UPSERT_QUERY, ("host-1", 2, '{"port": 443, "status": "OPEN"}', t1))
        self.conn.commit()

        # 2. Incoming newer packet with version 3
        self.cursor.execute(UPSERT_QUERY, ("host-1", 3, '{"port": 443, "status": "CLOSED"}', t2))
        self.conn.commit()

        # 3. Assert state updated to version 3
        row = self.get_record("host-1")
        self.assertEqual(row[1], 3)
        self.assertEqual(row[2], '{"port": 443, "status": "CLOSED"}')

    def test_idempotent_retry_same_version(self):
        """Verify that receiving the same version again is idempotent."""
        t1 = 100.0
        t2 = 105.0

        self.cursor.execute(UPSERT_QUERY, ("host-1", 2, '{"port": 443, "status": "OPEN"}', t1))
        self.conn.commit()

        # Retry with version 2 (e.g. at-least-once delivery retry)
        self.cursor.execute(UPSERT_QUERY, ("host-1", 2, '{"port": 443, "status": "OPEN"}', t2))
        self.conn.commit()

        # Assert only 1 row exists in table (no duplicates)
        self.cursor.execute("SELECT COUNT(*) FROM scans WHERE id = 'host-1'")
        count = self.cursor.fetchone()[0]
        self.assertEqual(count, 1)

if __name__ == "__main__":
    unittest.main()