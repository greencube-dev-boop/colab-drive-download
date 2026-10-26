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
from media_search import search_media_imdb, get_tv_seasons_data
import ui

class TestTwoStagePipeline(unittest.TestCase):

    def setUp(self):
        config.refine_worker_active = False
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
        id1 = database.enqueue_refine({"title": "Episode 1", "raw_drive_path": "/fake/1", "refining_config": {}})
        id2 = database.enqueue_refine({"title": "Episode 2", "raw_drive_path": "/fake/2", "refining_config": {}})
        id3 = database.enqueue_refine({"title": "Episode 3", "raw_drive_path": "/fake/3", "refining_config": {}})

        q = database.get_refine_queue()
        self.assertEqual(len(q), 3)
        self.assertEqual(q[0]["id"], id1)
        self.assertEqual(q[1]["id"], id2)
        self.assertEqual(q[2]["id"], id3)

        moved = database.move_queue_item("refine_queue", id2, "up")
        self.assertTrue(moved)

        q_after = database.get_refine_queue()
        self.assertEqual(q_after[0]["id"], id2)
        self.assertEqual(q_after[1]["id"], id1)

        moved_down = database.move_queue_item("refine_queue", id2, "down")
        self.assertTrue(moved_down)

        q_after2 = database.get_refine_queue()
        self.assertEqual(q_after2[0]["id"], id1)
        self.assertEqual(q_after2[1]["id"], id2)

        job = database.claim_next_refine_job()
        self.assertIsNotNone(job)
        self.assertEqual(job["id"], id1)
        self.assertEqual(job["status"], "refining")

        database.complete_refine_job(id1, final_size_mb=45.2)
        q_completed = database.get_refine_queue()
        completed_job = next(j for j in q_completed if j["id"] == id1)
        self.assertEqual(completed_job["status"], "completed")

        cleared = database.clear_completed("refine_queue")
        self.assertEqual(cleared, 1)
        self.assertEqual(len(database.get_refine_queue()), 2)

    def test_metadata_manager_schema_conformance(self):
        metadata_manager.record_item(
            media_type="movie",
            name="Avatar Fire And Ash (2025)",
            status="raw",
            raw_path="/drive/downloader/raw/Avatar.mp4",
            refined_path="",
            refining_data={"crf": 32, "res": "480p"}
        )

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

        show = data["tvseris"][0]
        self.assertEqual(show["name"], "Mentalist")
        self.assertIn("season_01", show["seasons"])
        self.assertIn("episode_04", show["seasons"]["season_01"])

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

    def test_deduplication_pre_and_post_download(self):
        import pipeline

        # 1. First Task: Download Mentalist S01E01 via Direct Link
        mock_raw_1 = os.path.join(config.LOCAL_RAW_DIR, "task_1_Mentalist.S01E01.720p.WEB-DL.mkv")
        with open(mock_raw_1, "wb") as f:
            f.write(b"episode_1_direct_bytes")

        task_1 = {
            "id": 101,
            "db_id": 101,
            "title": "Mentalist.S01E01.720p.WEB-DL",
            "url": "https://videm.xyz/embed/tv/tt1196946/1/1",
            "raw_path": mock_raw_1,
            "res_preset": "480p",
            "video_codec": "H.265 / HEVC",
            "container_format": "mkv",
            "audio_preset": "Opus 64 kbps",
            "crf_val": 32,
            "parallel_workers": 2,
            "cancelled": False
        }

        with patch("pipeline.download_worker", lambda t: None):
            pipeline.run_download_pipeline(task_1)

        # Confirm Task 1 is staged
        expected_raw_1 = os.path.join(config.RAW_DIR, "Mentalist S01E01.mkv")
        self.assertTrue(os.path.exists(expected_raw_1))

        # Check metadata has Mentalist S01E01 as raw
        meta = metadata_manager.load_metadata()
        self.assertEqual(meta["tvseris"][0]["seasons"]["season_01"]["episode_01"]["status"], "raw")

        # 2. Second Task: Duplicate added via Magnet swarm for same episode
        mock_raw_2 = os.path.join(config.LOCAL_RAW_DIR, "task_2_The.Mentalist.S01E01.HDTV.mkv")
        with open(mock_raw_2, "wb") as f:
            f.write(b"episode_1_duplicate_bytes")

        task_2 = {
            "id": 102,
            "db_id": 102,
            "title": "The.Mentalist.S01E01.HDTV",
            "url": "magnet:?xt=urn:btih:FAKE1234567890",
            "raw_path": mock_raw_2,
            "res_preset": "480p",
            "video_codec": "H.265 / HEVC",
            "container_format": "mkv",
            "audio_preset": "Opus 64 kbps",
            "crf_val": 32,
            "parallel_workers": 2,
            "cancelled": False
        }

        # Run pipeline on duplicate task
        pipeline.run_download_pipeline(task_2)

        # Verify Deduplication:
        # Task 2 cancelled due to duplicate detection
        self.assertTrue(task_2.get("cancelled"))
        self.assertIn("Already in Drive", task_2.get("status"))

        # Verify duplicate was deleted from local disk
        self.assertFalse(os.path.exists(mock_raw_2))

        # Verify refine_queue has ONLY 1 job (Task 1), no duplicate
        ref_q = database.get_refine_queue()
        self.assertEqual(len(ref_q), 1)

    def test_prune_pending_duplicates(self):
        # Enqueue 3 items in download_queue:
        # A: Mentalist S01E01 (Direct)
        # B: The Mentalist S01E01 (Magnet) - Duplicate!
        # C: The Mentalist S01E02 (Magnet) - Different episode
        id_a = database.enqueue_download({"title": "Mentalist S01E01", "url": "http://direct"})
        id_b = database.enqueue_download({"title": "The.Mentalist.S01E01.HDTV", "url": "magnet:b"})
        id_c = database.enqueue_download({"title": "The.Mentalist.S01E02.HDTV", "url": "magnet:c"})

        q_before = database.get_download_queue()
        self.assertEqual(len(q_before), 3)

        # Media info for the one being completed/staged
        media_info = classifier.classify_media("Mentalist.S01E01")
        cancelled_count = metadata_manager.prune_pending_duplicates(media_info)

        # Should prune exactly item B (duplicate S01E01), leaving C (S01E02) pending!
        self.assertEqual(cancelled_count, 2) # id_a and id_b both match S01E01

        q_after = database.get_download_queue()
        item_c = next(i for i in q_after if i["id"] == id_c)
        self.assertEqual(item_c["status"], "pending")

        item_b = next(i for i in q_after if i["id"] == id_b)
        self.assertEqual(item_b["status"], "cancelled")
        self.assertIn("Duplicate", item_b["error_msg"])

    def test_search_media_data_flow_without_list_attribute_error(self):
        choices, cache, msg = search_media_imdb("Mentalist", media_type="📺 TV Series")
        self.assertIsInstance(choices, list)
        self.assertIsInstance(cache, dict)
        self.assertTrue(len(choices) > 0)

        # Verify every item in cache is a dict, not a list
        results = [cache[label] for label in choices]
        for r in results:
            self.assertIsInstance(r, dict)
            self.assertIn("type", r)
            self.assertEqual(r["type"], "tv")
            self.assertIn("id", r)

        # Verify get_tv_seasons_data returns (title, seasons_dict)
        first_id = results[0]["id"]
        show_title, seasons = get_tv_seasons_data(first_id)
        self.assertIsInstance(show_title, str)
        self.assertIsInstance(seasons, dict)
        self.assertTrue(len(seasons) > 0)

if __name__ == "__main__":
    unittest.main()
