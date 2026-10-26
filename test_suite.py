import os
import sys
import json
import unittest
import tempfile
import shutil
from unittest.mock import patch, MagicMock

# Ensure current directory is on PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import config
import database
import metadata_manager
import classifier
from torrent_parser import format_torrent_size, bdecode, parse_torrent_bytes
import ui

class TestTwoStagePipeline(unittest.TestCase):

    def setUp(self):
        config.refine_worker_active = False
        # Use isolated temp directories for testing
        self.test_dir = tempfile.mkdtemp()
        self.orig_downloader = config.DOWNLOADER_DIR
        self.orig_raw = config.RAW_DIR
        self.orig_refined = config.REFINED_DIR
        self.orig_data = config.DATA_DIR
        self.orig_db = config.QUEUE_DB
        self.orig_meta = config.METADATA_JSON
        self.orig_trash = config.TRASH_STAGING_DIR
        self.orig_local_raw = config.LOCAL_RAW_DIR
        self.orig_local_refine = config.LOCAL_REFINE_DIR

        config.DOWNLOADER_DIR = self.test_dir
        config.RAW_DIR = os.path.join(self.test_dir, "raw")
        config.REFINED_DIR = os.path.join(self.test_dir, "refined")
        config.REFINED_MOVIES_DIR = os.path.join(config.REFINED_DIR, "movies")
        config.REFINED_TV_DIR = os.path.join(config.REFINED_DIR, "tv_series")
        config.REFINED_OTHER_DIR = os.path.join(config.REFINED_DIR, "other")
        config.DATA_DIR = os.path.join(self.test_dir, "data")
        config.QUEUE_DB = os.path.join(config.DATA_DIR, "queue.db")
        config.METADATA_JSON = os.path.join(config.DATA_DIR, "metadata.json")
        config.TRASH_STAGING_DIR = os.path.join(self.test_dir, "trash_staging")
        config.LOCAL_RAW_DIR = os.path.join(self.test_dir, "local_raw")
        config.LOCAL_REFINE_DIR = os.path.join(self.test_dir, "local_refine")

        for d in [config.RAW_DIR, config.REFINED_DIR, config.REFINED_MOVIES_DIR,
                  config.REFINED_TV_DIR, config.REFINED_OTHER_DIR, config.DATA_DIR,
                  config.TRASH_STAGING_DIR, config.LOCAL_RAW_DIR, config.LOCAL_REFINE_DIR]:
            os.makedirs(d, exist_ok=True)

        database.init_db()

    def tearDown(self):
        config.refine_worker_active = False
        config.DOWNLOADER_DIR = self.orig_downloader
        config.RAW_DIR = self.orig_raw
        config.REFINED_DIR = self.orig_refined
        config.DATA_DIR = self.orig_data
        config.QUEUE_DB = self.orig_db
        config.METADATA_JSON = self.orig_meta
        config.TRASH_STAGING_DIR = self.orig_trash
        config.LOCAL_RAW_DIR = self.orig_local_raw
        config.LOCAL_REFINE_DIR = self.orig_local_refine
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_classifier_movie(self):
        name = "Avatar.Fire.And.Ash.2025.720p.WEBRip.x264.AAC-[YTS.BZ].mp4"
        res = classifier.classify_media(name)
        self.assertEqual(res["type"], "movie")
        self.assertEqual(res["year"], 2025)
        self.assertIn("Avatar Fire And Ash (2025)", res["formatted_name"])

    def test_classifier_tv_standard(self):
        name = "Mentalist.S01E04.720p.WEB-DL.x264.mkv"
        res = classifier.classify_media(name)
        self.assertEqual(res["type"], "tv_series")
        self.assertEqual(res["season"], 1)
        self.assertEqual(res["episode"], 4)
        self.assertEqual(res["show_name"], "Mentalist")
        self.assertEqual(res["formatted_name"], "Mentalist S01E04")

    def test_classifier_tv_alternate_patterns(self):
        name = "The.Mentalist.1x04.HDTV.x264"
        res = classifier.classify_media(name)
        self.assertEqual(res["type"], "tv_series")
        self.assertEqual(res["season"], 1)
        self.assertEqual(res["episode"], 4)
        self.assertEqual(res["formatted_name"], "The Mentalist S01E04")

        name2 = "Breaking Bad Season 2 Episode 10 1080p"
        res2 = classifier.classify_media(name2)
        self.assertEqual(res2["type"], "tv_series")
        self.assertEqual(res2["season"], 2)
        self.assertEqual(res2["episode"], 10)
        self.assertEqual(res2["formatted_name"], "Breaking Bad S02E10")

    def test_database_queue_operations_and_reordering(self):
        # 1. Enqueue items
        id1 = database.enqueue_refine({"title": "Episode 1", "raw_drive_path": "/fake/1", "refining_config": {}})
        id2 = database.enqueue_refine({"title": "Episode 2", "raw_drive_path": "/fake/2", "refining_config": {}})
        id3 = database.enqueue_refine({"title": "Episode 3", "raw_drive_path": "/fake/3", "refining_config": {}})

        q = database.get_refine_queue()
        self.assertEqual(len(q), 3)
        self.assertEqual(q[0]["id"], id1)
        self.assertEqual(q[1]["id"], id2)
        self.assertEqual(q[2]["id"], id3)

        # 2. Reorder Episode 2 UP
        moved = database.move_queue_item("refine_queue", id2, "up")
        self.assertTrue(moved)

        q_after = database.get_refine_queue()
        self.assertEqual(q_after[0]["id"], id2)
        self.assertEqual(q_after[1]["id"], id1)
        self.assertEqual(q_after[2]["id"], id3)

        # 3. Reorder Episode 2 DOWN
        moved_down = database.move_queue_item("refine_queue", id2, "down")
        self.assertTrue(moved_down)

        q_after2 = database.get_refine_queue()
        self.assertEqual(q_after2[0]["id"], id1)
        self.assertEqual(q_after2[1]["id"], id2)

        # 4. Claim next job
        job = database.claim_next_refine_job()
        self.assertIsNotNone(job)
        self.assertEqual(job["id"], id1)
        self.assertEqual(job["status"], "refining")

        # 5. Complete job
        database.complete_refine_job(id1, final_size_mb=45.2)
        q_completed = database.get_refine_queue()
        completed_job = next(j for j in q_completed if j["id"] == id1)
        self.assertEqual(completed_job["status"], "completed")
        self.assertEqual(completed_job["convert_pct"], 100.0)

        # 6. Clear completed
        cleared = database.clear_completed("refine_queue")
        self.assertEqual(cleared, 1)
        self.assertEqual(len(database.get_refine_queue()), 2)

    def test_metadata_manager_schema_conformance(self):
        # 1. Record Movie
        metadata_manager.record_item(
            media_type="movie",
            name="Avatar Fire And Ash (2025)",
            status="raw",
            raw_path="/drive/downloader/raw/Avatar.mp4",
            refined_path="",
            refining_data={"crf": 32, "res": "480p"}
        )

        # 2. Record TV Episode
        metadata_manager.record_item(
            media_type="tv_series",
            name="Mentalist S01E04",
            status="raw",
            raw_path="/drive/downloader/raw/Mentalist S01E04.mkv",
            refined_path="",
            refining_data={"crf": 32, "res": "480p"},
            show_name="Mentalist",
            season=1,
            episode=4
        )

        data = metadata_manager.load_metadata()
        self.assertIn("movies", data)
        self.assertIn("tvseris", data)
        self.assertEqual(len(data["movies"]), 1)
        self.assertEqual(data["movies"][0]["name"], "Avatar Fire And Ash (2025)")
        self.assertEqual(data["movies"][0]["status"], "raw")

        show = data["tvseris"][0]
        self.assertEqual(show["name"], "Mentalist")
        self.assertIn("season_01", show["seasons"])
        self.assertIn("episode_04", show["seasons"]["season_01"])
        self.assertEqual(show["seasons"]["season_01"]["episode_04"]["status"], "raw")

        # 3. Update to Refined
        metadata_manager.update_item_status(
            media_type="tv_series",
            name="Mentalist S01E04",
            status="refined",
            refined_path="/drive/downloader/refined/tv_series/Mentalist/Season 01/Mentalist S01E04.mkv",
            show_name="Mentalist",
            season=1,
            episode=4
        )
        data2 = metadata_manager.load_metadata()
        ep = data2["tvseris"][0]["seasons"]["season_01"]["episode_04"]
        self.assertEqual(ep["status"], "refined")
        self.assertIn("Mentalist S01E04.mkv", ep["refined_path"])

    def test_trash_bin_bypass_delete(self):
        raw_file = os.path.join(config.RAW_DIR, "mock_test_video.mkv")
        with open(raw_file, "wb") as f:
            f.write(b"0" * 1024 * 100)
        self.assertTrue(os.path.exists(raw_file))

        success = config.bypass_trash_delete(raw_file)
        self.assertTrue(success)
        self.assertFalse(os.path.exists(raw_file))

        staging_files = os.listdir(config.TRASH_STAGING_DIR)
        self.assertEqual(len(staging_files), 0)

    def test_ui_creation(self):
        demo = ui.create_ui()
        self.assertIsNotNone(demo)
        self.assertEqual(demo.title, "Google Drive Video Downloader & GPU Refiner")

    def test_end_to_end_mock_stage1_and_stage2(self):
        import pipeline

        # 1. Create mock downloaded file
        mock_raw = os.path.join(config.LOCAL_RAW_DIR, "task_99_Avatar.Fire.And.Ash.2025.720p.WEBRip.mp4")
        with open(mock_raw, "wb") as f:
            f.write(b"video_bytes_12345")

        task = {
            "id": 99,
            "db_id": 99,
            "title": "Avatar.Fire.And.Ash.2025.720p.WEBRip",
            "url": "https://example.com/mock.mp4",
            "raw_path": mock_raw,
            "res_preset": "480p",
            "video_codec": "H.265 / HEVC",
            "container_format": "mkv",
            "audio_preset": "Opus 64 kbps",
            "crf_val": 32,
            "parallel_workers": 2,
            "cancelled": False
        }

        # Mock download_worker so it skips actual network call and uses the pre-created mock_raw
        with patch("pipeline.download_worker", lambda t: None):
            pipeline.run_download_pipeline(task)

        # Verify Stage 1 results:
        # File moved to Drive raw/ with standardized name
        expected_raw_path = os.path.join(config.RAW_DIR, "Avatar Fire And Ash (2025).mp4")
        self.assertTrue(os.path.exists(expected_raw_path))

        # Recorded in metadata.json with status="raw"
        meta = metadata_manager.load_metadata()
        self.assertEqual(len(meta["movies"]), 1)
        self.assertEqual(meta["movies"][0]["status"], "raw")
        self.assertEqual(meta["movies"][0]["name"], "Avatar Fire And Ash (2025)")

        # Enqueued in refine_queue
        ref_q = database.get_refine_queue()
        self.assertEqual(len(ref_q), 1)
        self.assertEqual(ref_q[0]["status"], "pending")
        self.assertEqual(ref_q[0]["title"], "Avatar Fire And Ash (2025)")

        # 2. Test Stage 2: Refining Worker
        job = database.claim_next_refine_job()
        self.assertIsNotNone(job)

        # Mock convert_worker to produce fake transcoded output
        def fake_convert(r_task):
            out = os.path.join(config.LOCAL_REFINE_DIR, "fake_out.mkv")
            with open(out, "wb") as f:
                f.write(b"encoded_mkv_bytes_999")
            r_task["temp_path"] = out

        with patch("pipeline.convert_worker", fake_convert):
            pipeline._refine_job_worker(job)

        # Verify Stage 2 results:
        # Refined output moved to structured Drive path
        expected_refined_path = os.path.join(config.REFINED_MOVIES_DIR, "Avatar Fire And Ash (2025).mkv")
        self.assertTrue(os.path.exists(expected_refined_path))

        # Raw file deleted with Trash Bin bypass!
        self.assertFalse(os.path.exists(expected_raw_path))

        # Metadata updated to "refined"
        meta_updated = metadata_manager.load_metadata()
        self.assertEqual(meta_updated["movies"][0]["status"], "refined")
        self.assertEqual(meta_updated["movies"][0]["refined_path"], expected_refined_path)

        # Job in queue.db marked "completed"
        ref_q_after = database.get_refine_queue()
        self.assertEqual(ref_q_after[0]["status"], "completed")

if __name__ == "__main__":
    unittest.main()
