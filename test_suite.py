import os
import sys
import time
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
from downloader import DownloadWatchdog, parse_speed_to_bps, parse_size_to_bytes
import ui

class TestTwoStagePipeline(unittest.TestCase):

    def setUp(self):
        config.download_worker_active = False
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
        self.orig_drive_db = getattr(config, "DRIVE_QUEUE_DB", None)
        self.orig_local_db = getattr(config, "LOCAL_QUEUE_DB", None)
        self.orig_local_data = getattr(config, "LOCAL_DATA_DIR", None)
        self.orig_local_meta = getattr(config, "LOCAL_METADATA_JSON", None)
        self.orig_is_colab = getattr(config, "IS_COLAB", False)
        self.orig_cluster_dir = getattr(config, "CLUSTER_DIR", None)
        self.orig_cluster_workers_dir = getattr(config, "CLUSTER_WORKERS_DIR", None)
        self.orig_cluster_master_file = getattr(config, "CLUSTER_MASTER_FILE", None)

        config.DOWNLOADER_DIR = self.test_dir
        config.RAW_DIR = os.path.join(self.test_dir, "raw")
        config.REFINED_DIR = os.path.join(self.test_dir, "refined")
        config.REFINED_MOVIES_DIR = os.path.join(config.REFINED_DIR, "movies")
        config.REFINED_TV_DIR = os.path.join(config.REFINED_DIR, "tv_series")
        config.REFINED_OTHER_DIR = os.path.join(config.REFINED_DIR, "other")
        config.DATA_DIR = os.path.join(self.test_dir, "data")
        config.LOCAL_DATA_DIR = os.path.join(self.test_dir, "local_data")
        config.QUEUE_DB = os.path.join(config.DATA_DIR, "queue.db")
        config.DRIVE_QUEUE_DB = os.path.join(config.DATA_DIR, "queue.db")
        config.LOCAL_QUEUE_DB = os.path.join(config.LOCAL_DATA_DIR, "queue.db")
        config.METADATA_JSON = os.path.join(config.DATA_DIR, "metadata.json")
        config.LOCAL_METADATA_JSON = os.path.join(config.LOCAL_DATA_DIR, "metadata.json")
        config.TRASH_STAGING_DIR = os.path.join(self.test_dir, "trash_staging")
        config.LOCAL_RAW_DIR = os.path.join(self.test_dir, "local_raw")
        config.LOCAL_REFINE_DIR = os.path.join(self.test_dir, "local_refine")
        config.CLUSTER_DIR = os.path.join(self.test_dir, "data", "cluster")
        config.CLUSTER_WORKERS_DIR = os.path.join(config.CLUSTER_DIR, "workers")
        config.CLUSTER_MASTER_FILE = os.path.join(config.CLUSTER_DIR, "master.json")
        config.LOCAL_CLUSTER_DIR = os.path.join(self.test_dir, "local_data", "cluster")
        config.LOCAL_CLUSTER_WORKERS_DIR = os.path.join(config.LOCAL_CLUSTER_DIR, "workers")
        config.LOCAL_CLUSTER_MASTER_FILE = os.path.join(config.LOCAL_CLUSTER_DIR, "master.json")

        for d in [config.RAW_DIR, config.REFINED_DIR, config.REFINED_MOVIES_DIR,
                  config.REFINED_TV_DIR, config.REFINED_OTHER_DIR, config.DATA_DIR, config.LOCAL_DATA_DIR,
                  config.TRASH_STAGING_DIR, config.LOCAL_RAW_DIR, config.LOCAL_REFINE_DIR,
                  config.CLUSTER_DIR, config.CLUSTER_WORKERS_DIR, config.LOCAL_CLUSTER_DIR, config.LOCAL_CLUSTER_WORKERS_DIR]:
            os.makedirs(d, exist_ok=True)

        metadata_manager.reset_cache()
        database.init_db()

    def tearDown(self):
        config.download_worker_active = False
        config.refine_worker_active = False
        metadata_manager.reset_cache()
        config.DOWNLOADER_DIR = self.orig_downloader
        config.RAW_DIR = self.orig_raw
        config.REFINED_DIR = self.orig_refined
        config.DATA_DIR = self.orig_data
        config.QUEUE_DB = self.orig_db
        config.METADATA_JSON = self.orig_meta
        config.TRASH_STAGING_DIR = self.orig_trash
        config.LOCAL_RAW_DIR = self.orig_local_raw
        config.LOCAL_REFINE_DIR = self.orig_local_refine
        config.DRIVE_QUEUE_DB = self.orig_drive_db
        config.LOCAL_QUEUE_DB = self.orig_local_db
        config.LOCAL_DATA_DIR = self.orig_local_data
        config.LOCAL_METADATA_JSON = self.orig_local_meta
        config.IS_COLAB = self.orig_is_colab
        config.CLUSTER_DIR = self.orig_cluster_dir
        config.CLUSTER_WORKERS_DIR = self.orig_cluster_workers_dir
        config.CLUSTER_MASTER_FILE = self.orig_cluster_master_file
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

    def test_ui_queue_rendering_and_history_retention(self):
        # 1. Add download tasks: 1 pending, 1 failed, 1 cancelled, 1 completed
        id_pending = database.enqueue_download({"title": "Upcoming Show S01E02", "url": "http://link2"})
        id_failed = database.enqueue_download({"title": "Broken Stream S01E03", "url": "http://broken"})
        database.update_item_status("download_queue", id_failed, status="failed", error_msg="HTTP 404 Not Found")
        
        id_cancelled = database.enqueue_download({"title": "Duplicate Show S01E01", "url": "http://dup"})
        database.update_item_status("download_queue", id_cancelled, status="cancelled", error_msg="Duplicate detected in Drive")
        
        id_staged = database.enqueue_download({"title": "Completed Show S01E01", "url": "http://staged"})
        database.update_item_status("download_queue", id_staged, status="completed")

        dl_html = ui.render_download_queue_html()
        self.assertIn("Upcoming Show S01E02", dl_html)
        self.assertIn("Broken Stream S01E03", dl_html)
        self.assertIn("HTTP 404 Not Found", dl_html)
        self.assertIn("Duplicate Show S01E01", dl_html)
        self.assertIn("Duplicate detected in Drive", dl_html)
        self.assertIn("Completed Show S01E01", dl_html)

        # Inverted history assertion (nearest / most recent at top)
        pos_staged = dl_html.find("Completed Show S01E01")
        pos_cancelled = dl_html.find("Duplicate Show S01E01")
        pos_failed = dl_html.find("Broken Stream S01E03")
        self.assertTrue(pos_staged < pos_cancelled < pos_failed, "Download history not ordered nearest-first (descending)!")

        # 2. Add refine tasks: 1 pending, 1 failed with transcode error, 1 completed
        rf_p = database.enqueue_refine({"title": "Refine Pending", "raw_drive_path": "/r/1"})
        rf_f = database.enqueue_refine({"title": "Refine Broken NVENC", "raw_drive_path": "/r/2"})
        database.update_item_status("refine_queue", rf_f, status="failed", error_msg="NVENC Out of Memory")
        rf_c = database.enqueue_refine({"title": "Refine Done 1080p", "raw_drive_path": "/r/3"})
        database.complete_refine_job(rf_c, final_size_mb=120.5)

        rf_html = ui.render_refine_queue_html()
        self.assertIn("Refine Pending", rf_html)
        self.assertIn("Refine Broken NVENC", rf_html)
        self.assertIn("NVENC Out of Memory", rf_html)
        self.assertIn("Refine Done 1080p", rf_html)
        self.assertIn("120.5 MB", rf_html)

        # Inverted history assertion for refine queue (most recent completion on top)
        pos_ref_done = rf_html.find("Refine Done 1080p")
        pos_ref_broken = rf_html.find("Refine Broken NVENC")
        self.assertTrue(pos_ref_done < pos_ref_broken, "Refine history not ordered nearest-first (descending)!")

    def test_catalog_ascending_numerical_sorting(self):
        # Record episodes in reverse / random order: Episode 4, Episode 1, Episode 2
        metadata_manager.record_item(
            media_type="tv_series",
            name="Sherlock S01E04",
            status="raw",
            raw_path="/raw/Sherlock S01E04.mkv",
            show_name="Sherlock",
            season=1,
            episode=4
        )
        metadata_manager.record_item(
            media_type="tv_series",
            name="Sherlock S01E01",
            status="refined",
            raw_path="",
            refined_path="/refined/Sherlock S01E01.mkv",
            show_name="Sherlock",
            season=1,
            episode=1
        )
        metadata_manager.record_item(
            media_type="tv_series",
            name="Sherlock S01E02",
            status="raw",
            raw_path="/raw/Sherlock S01E02.mkv",
            show_name="Sherlock",
            season=1,
            episode=2
        )

        catalog_html = ui.render_catalog_html()
        self.assertIn("Sherlock S01E01", catalog_html)
        self.assertIn("Sherlock S01E02", catalog_html)
        self.assertIn("Sherlock S01E04", catalog_html)

        # Check ascending order in HTML string: E01 must appear before E02, which must appear before E04
        pos_e1 = catalog_html.find("Sherlock S01E01")
        pos_e2 = catalog_html.find("Sherlock S01E02")
        pos_e4 = catalog_html.find("Sherlock S01E04")
        self.assertTrue(pos_e1 < pos_e2 < pos_e4, f"Episodes not ascending: E01 at {pos_e1}, E02 at {pos_e2}, E04 at {pos_e4}")

    def test_cancel_or_dismiss_task(self):
        import pipeline
        # Create a completed task in download_queue
        id_item = database.enqueue_download({"title": "Finished Item", "url": "http://fin"})
        database.update_item_status("download_queue", id_item, status="completed")

        # Cancelling with dismiss=False does not delete completed item
        pipeline.cancel_task_by_id(id_item, table_name="download_queue", dismiss=False)
        self.assertIsNotNone(database.get_queue_item("download_queue", id_item))

        # Explicit dismiss deletes it from the table
        dismissed = pipeline.cancel_task_by_id(id_item, table_name="download_queue", dismiss=True)
        self.assertTrue(dismissed)
        self.assertIsNone(database.get_queue_item("download_queue", id_item))

        # Cancelling an active/pending task should mark it cancelled with reason and keep in DB
        id_pending = database.enqueue_download({"title": "Active Pending", "url": "http://act"})
        cancelled = pipeline.cancel_task_by_id(id_pending, table_name="download_queue", dismiss=False)
        self.assertTrue(cancelled)
        item = database.get_queue_item("download_queue", id_pending)
        self.assertEqual(item["status"], "cancelled")
        self.assertEqual(item["error_msg"], "Cancelled by user")

    def test_torrent_selective_title_preservation(self):
        import pipeline
        # Task representing an inspected video episode
        task = {
            "id": 999,
            "db_id": 999,
            "title": "Mentalist S01E02",
            "url": "magnet:?xt=urn:btih:ABCDEF1234567890ABCDEF1234567890ABCDEF12&dn=The.Mentalist.Season.1.Complete",
            "has_custom_title": True,
            "torrent_file_index": 2,
            "target_file_index": 2,
            "cancelled": False
        }

        # Mock resolve_video_source and download_aria2c_torrent
        with patch("pipeline.resolve_video_source") as mock_resolve, \
             patch("pipeline.download_aria2c_torrent") as mock_aria:
            mock_resolve.return_value = {
                "url": task["url"],
                "type": "local_torrent",
                "title": "The.Mentalist.Season.1.Complete",
                "headers": {}
            }
            fake_video = os.path.join(config.LOCAL_RAW_DIR, "fake_dl.mkv")
            with open(fake_video, "wb") as f:
                f.write(b"fake video data 123456")
            mock_aria.return_value = fake_video

            pipeline.download_worker(task)

            # Crucial assertion: task["title"] MUST NOT be overwritten by "The.Mentalist.Season.1.Complete"
            self.assertEqual(task["title"], "Mentalist S01E02")
            self.assertTrue(os.path.exists(task["raw_path"]))
            self.assertIn("Mentalist S01E02", os.path.basename(task["raw_path"]))

    def test_torrent_auto_split_enqueues_individual_episodes(self):
        import pipeline
        magnet = "magnet:?xt=urn:btih:1111111111111111111111111111111111111111&dn=Show.Season.1"

        mock_meta = {
            "name": "Show.Season.1",
            "cached_file_path": "/fake/cached.torrent",
            "files": [
                {"index": 1, "filename": "Show.S01E01.mkv", "filename_clean": "Show S01E01", "is_video": True, "folder": "Root"},
                {"index": 2, "filename": "Show.S01E02.mkv", "filename_clean": "Show S01E02", "is_video": True, "folder": "Root"},
                {"index": 3, "filename": "sample.mp4", "filename_clean": "sample", "is_video": False, "folder": "Root"}
            ]
        }

        with patch("torrent_parser.fetch_torrent_metadata", return_value=mock_meta):
            count = pipeline.queue_direct_items([(magnet, "")])
            # Should enqueue 2 video episodes, ignoring the sample
            self.assertEqual(count, 2)

            q_items = database.get_download_queue()
            titles = [i["title"] for i in q_items]
            self.assertIn("Show S01E01", titles)
            self.assertIn("Show S01E02", titles)

    def test_download_start_timeout_requeue_and_fail(self):
        # 1. Enqueue job 1 and job 2
        id1 = database.enqueue_download({"title": "Timeout Movie 1", "url": "http://slow1"})
        id2 = database.enqueue_download({"title": "Upcoming Movie 2", "url": "http://slow2"})

        q = database.get_download_queue()
        self.assertEqual(len(q), 2)
        self.assertEqual(q[0]["id"], id1)
        self.assertEqual(q[0]["retry_count"], 0)

        # Retry 1: requeue id1
        requeued, retry_num = database.requeue_download_for_retry(id1, max_retries=2)
        self.assertTrue(requeued)
        self.assertEqual(retry_num, 1)

        # Confirm id1 moved to the END of the queue, after id2
        q1 = database.get_download_queue()
        self.assertEqual(q1[0]["id"], id2)
        self.assertEqual(q1[1]["id"], id1)
        self.assertEqual(q1[1]["retry_count"], 1)
        self.assertEqual(q1[1]["status"], "pending")

        # Retry 2: requeue id1 again
        requeued2, retry_num2 = database.requeue_download_for_retry(id1, max_retries=2)
        self.assertTrue(requeued2)
        self.assertEqual(retry_num2, 2)
        item1 = database.get_queue_item("download_queue", id1)
        self.assertEqual(item1["retry_count"], 2)
        self.assertEqual(item1["status"], "pending")

        # Retry 3: 3rd attempt exceeds max_retries=2 -> must fail
        requeued3, retry_num3 = database.requeue_download_for_retry(id1, max_retries=2)
        self.assertFalse(requeued3)
        self.assertEqual(retry_num3, 3)

        item1_failed = database.get_queue_item("download_queue", id1)
        self.assertEqual(item1_failed["status"], "failed")
        self.assertIn("3 attempts", item1_failed["error_msg"])

    def test_low_speed_demote_and_dual_queue_lanes(self):
        # 1. Enqueue 2 high-speed jobs
        id_fast = database.enqueue_download({"title": "Fast Torrent S01E01", "url": "magnet:fast", "is_low_speed": 0})
        id_crawl = database.enqueue_download({"title": "Crawling Torrent S01E02", "url": "magnet:crawl", "is_low_speed": 0})

        # Claim the first high-speed job
        claimed_high = database.claim_next_download_job(is_low_speed=False)
        self.assertIsNotNone(claimed_high)
        self.assertEqual(claimed_high["id"], id_fast)

        # Demote the crawling torrent to low speed trickle queue
        demoted = database.demote_to_low_speed_queue(id_crawl, reason="< 100 KB/s for 120s")
        self.assertTrue(demoted)

        crawl_item = database.get_queue_item("download_queue", id_crawl)
        self.assertEqual(crawl_item["is_low_speed"], 1)
        self.assertEqual(crawl_item["status"], "pending")
        self.assertIn("100 KB/s", crawl_item["error_msg"])

        # High-speed daemon should find NO more high-speed jobs (since id_fast is downloading, and id_crawl is low-speed)
        next_high = database.claim_next_download_job(is_low_speed=False)
        self.assertIsNone(next_high)

        # Low-speed daemon should successfully claim id_crawl
        claimed_low = database.claim_next_download_job(is_low_speed=True)
        self.assertIsNotNone(claimed_low)
        self.assertEqual(claimed_low["id"], id_crawl)

        # Test toggle_download_queue_speed
        # Reset job to pending, then toggle back to fast lane
        database.update_item_status("download_queue", id_crawl, status="pending")
        toggled_to_fast = database.toggle_download_queue_speed(id_crawl)
        self.assertTrue(toggled_to_fast)
        self.assertEqual(database.get_queue_item("download_queue", id_crawl)["is_low_speed"], 0)

        # Toggle back to slow lane
        toggled_to_slow = database.toggle_download_queue_speed(id_crawl)
        self.assertTrue(toggled_to_slow)
        self.assertEqual(database.get_queue_item("download_queue", id_crawl)["is_low_speed"], 1)

        # Test unlimited concurrent capacity in low-speed queue
        id_s1 = database.enqueue_download({"title": "Slow 1", "url": "magnet:s1", "is_low_speed": 1})
        id_s2 = database.enqueue_download({"title": "Slow 2", "url": "magnet:s2", "is_low_speed": 1})
        id_s3 = database.enqueue_download({"title": "Slow 3", "url": "magnet:s3", "is_low_speed": 1})

        c1 = database.claim_next_download_job(is_low_speed=True)
        c2 = database.claim_next_download_job(is_low_speed=True)
        c3 = database.claim_next_download_job(is_low_speed=True)
        self.assertIsNotNone(c1)
        self.assertIsNotNone(c2)
        self.assertIsNotNone(c3)
        self.assertEqual(config.MAX_LOW_SPEED_DOWNLOAD_WORKERS, 0)

    def test_download_watchdog_timeout_detection(self):
        # 1. Test speed & size parsers
        self.assertEqual(parse_speed_to_bps("100KiB"), 100 * 1024)
        self.assertEqual(parse_speed_to_bps("1.5MiB"), 1.5 * 1024 * 1024)
        self.assertEqual(parse_speed_to_bps("500B"), 500)
        self.assertEqual(parse_size_to_bytes("50MiB"), 50 * 1024 * 1024)
        self.assertEqual(parse_size_to_bytes("1.2GiB"), int(1.2 * 1024 * 1024 * 1024))

        # 2. Test Watchdog Start Timeout: hasn't started after 90s
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        task_start_timeout = {"id": 201, "download_pct": 0, "is_low_speed": False, "active_proc": mock_proc}
        watchdog1 = DownloadWatchdog(task_start_timeout)
        # Simulate elapsed time of 95 seconds without download starting
        watchdog1.t_start = time.time() - (config.DOWNLOAD_START_TIMEOUT_SEC + 5)
        watchdog1.download_started = False
        triggered1 = watchdog1.check_conditions()

        self.assertTrue(triggered1)
        self.assertTrue(task_start_timeout.get("timed_out_start"))
        mock_proc.kill.assert_called()

        # 3. Test Watchdog Low Speed Demote: speed under 100KB/s for 120s
        mock_proc2 = MagicMock()
        mock_proc2.poll.return_value = None
        task_slow = {"id": 202, "download_pct": 10, "is_low_speed": False, "active_proc": mock_proc2}
        watchdog2 = DownloadWatchdog(task_slow)
        watchdog2.download_started = True
        # Simulate running for 125 seconds below 100KB/s
        watchdog2.low_speed_start = time.time() - (config.DOWNLOAD_LOW_SPEED_TIMEOUT_SEC + 5)
        triggered2 = watchdog2.check_conditions()

        self.assertTrue(triggered2)
        self.assertTrue(task_slow.get("demote_low_speed"))
        mock_proc2.kill.assert_called()

        # 4. Test Watchdog on Low-Speed lane: should NOT demote even if below 100KB/s
        mock_proc3 = MagicMock()
        mock_proc3.poll.return_value = None
        task_already_slow = {"id": 203, "download_pct": 5, "is_low_speed": True, "active_proc": mock_proc3}
        watchdog3 = DownloadWatchdog(task_already_slow)
        watchdog3.download_started = True
        watchdog3.low_speed_start = time.time() - (config.DOWNLOAD_LOW_SPEED_TIMEOUT_SEC + 5)
        triggered3 = watchdog3.check_conditions()

        self.assertFalse(triggered3)
        self.assertFalse(task_already_slow.get("demote_low_speed", False))
        mock_proc3.kill.assert_not_called()

    def test_unqueued_raw_files_detection_and_actions(self):
        import pipeline
        # 1. Initially no raw files -> no unqueued files detected, section does not appear in HTML
        unqueued = pipeline.get_unqueued_raw_files()
        self.assertEqual(len(unqueued), 0)
        html_initial = ui.render_refine_queue_html()
        self.assertNotIn("Un-queued Raw Files Detected", html_initial)

        # 2. Place 2 unqueued raw video files in config.RAW_DIR
        raw1 = os.path.join(config.RAW_DIR, "Inception.2010.720p.mkv")
        with open(raw1, "wb") as f:
            f.write(b"0" * 1024 * 50)

        raw2 = os.path.join(config.RAW_DIR, "Sherlock.S01E03.mkv")
        with open(raw2, "wb") as f:
            f.write(b"0" * 1024 * 75)

        # 3. Detect unqueued files
        detected = pipeline.get_unqueued_raw_files()
        self.assertEqual(len(detected), 2)
        filenames = [d["filename"] for d in detected]
        self.assertIn("Inception.2010.720p.mkv", filenames)
        self.assertIn("Sherlock.S01E03.mkv", filenames)

        # 4. Check UI renders the section with both files
        html_with_raw = ui.render_refine_queue_html()
        self.assertIn("Un-queued Raw Files Detected in Drive (2)", html_with_raw)
        self.assertIn("Inception.2010.720p.mkv", html_with_raw)
        self.assertIn("Sherlock.S01E03.mkv", html_with_raw)
        self.assertIn("➕ Add to Refine Queue", html_with_raw)
        self.assertIn("🗑️ Delete", html_with_raw)

        # 5. Enqueue the first file
        job_id = pipeline.enqueue_unqueued_raw_file("Inception.2010.720p.mkv")
        self.assertIsNotNone(job_id)

        # Now only 1 unqueued file remains
        detected_after_1 = pipeline.get_unqueued_raw_files()
        self.assertEqual(len(detected_after_1), 1)
        self.assertEqual(detected_after_1[0]["filename"], "Sherlock.S01E03.mkv")

        # Refine queue now has Inception in pending status
        q = database.get_refine_queue()
        inc_job = next(j for j in q if j["id"] == job_id)
        self.assertEqual(inc_job["status"], "pending")
        self.assertIn("Inception", inc_job["title"])

        # 6. Delete the second file
        deleted = pipeline.delete_unqueued_raw_file("Sherlock.S01E03.mkv")
        self.assertTrue(deleted)
        self.assertFalse(os.path.exists(raw2))

        # 7. Now 0 unqueued files remain -> section disappears completely!
        detected_after_all = pipeline.get_unqueued_raw_files()
        self.assertEqual(len(detected_after_all), 0)
        html_final = ui.render_refine_queue_html()
        self.assertNotIn("Un-queued Raw Files Detected", html_final)

    def test_drive_root_spill_prevention_and_cleanup(self):
        # 1. Verify resolve_downloader_dir never resolves to drive root
        config.IS_COLAB = True
        resolved = config.resolve_downloader_dir()
        forbidden = ["/content/drive/MyDrive", "/content/drive", "/content", "/"]
        self.assertNotIn(os.path.abspath(resolved), [os.path.abspath(f) for f in forbidden])
        self.assertTrue(resolved.endswith("downloader") or "downloader" in resolved or "downloader_staging" in resolved)

        # 2. Test cleanup_drive_root_stray_files
        mock_mydrive = os.path.join(self.test_dir, "mock_mydrive")
        os.makedirs(mock_mydrive, exist_ok=True)

        stray_db = os.path.join(mock_mydrive, "queue (1).db")
        stray_db_tmp = os.path.join(mock_mydrive, "queue.db.tmp")
        stray_meta_tmp = os.path.join(mock_mydrive, "metadata.json.tmp")
        stray_meta_conflict = os.path.join(mock_mydrive, "metadata (1).json")
        regular_file = os.path.join(mock_mydrive, "ImportantDocument.pdf")

        for fpath in [stray_db, stray_db_tmp, stray_meta_tmp, stray_meta_conflict, regular_file]:
            with open(fpath, "w", encoding="utf-8") as f:
                f.write("content")

        config.cleanup_drive_root_stray_files(drive_root=mock_mydrive)

        # Non-stray file must NOT be deleted
        self.assertTrue(os.path.exists(regular_file))
        # Stray files must be cleaned up
        self.assertFalse(os.path.exists(stray_db))
        self.assertFalse(os.path.exists(stray_db_tmp))
        self.assertFalse(os.path.exists(stray_meta_tmp))
        self.assertFalse(os.path.exists(stray_meta_conflict))

    def test_debounced_sync_and_zero_tmp_files(self):
        config.IS_COLAB = True
        config.LOCAL_QUEUE_DB = os.path.join(config.LOCAL_DATA_DIR, "queue.db")
        config.DRIVE_QUEUE_DB = os.path.join(config.DATA_DIR, "queue.db")
        config.QUEUE_DB = config.LOCAL_QUEUE_DB

        # Re-initialize DB in local queue path
        database.init_db()
        self.assertTrue(os.path.exists(config.LOCAL_QUEUE_DB))

        # Force sync to Drive
        database.sync_db_to_drive(force=True)
        self.assertTrue(os.path.exists(config.DRIVE_QUEUE_DB))

        # Verify NO .tmp file exists in Drive DATA_DIR
        drive_files = os.listdir(config.DATA_DIR)
        tmp_files = [f for f in drive_files if f.endswith(".tmp") or ".tmp" in f]
        self.assertEqual(len(tmp_files), 0)

        # Test metadata_manager save_metadata
        test_data = {"movies": [{"name": "Test Movie", "year": 2026}], "tvseris": [], "other": []}
        metadata_manager.save_metadata(test_data)

        # Verify metadata exists on Drive without any .tmp file
        self.assertTrue(os.path.exists(config.METADATA_JSON))
        drive_files_after_meta = os.listdir(config.DATA_DIR)
        tmp_files_after_meta = [f for f in drive_files_after_meta if f.endswith(".tmp") or ".tmp" in f]
        self.assertEqual(len(tmp_files_after_meta), 0)

        # Verify metadata_manager load_metadata uses cache
        loaded = metadata_manager.load_metadata()
        self.assertEqual(len(loaded["movies"]), 1)
        self.assertEqual(loaded["movies"][0]["name"], "Test Movie")

    def test_tv_series_differentiation_same_season_and_episode(self):
        import classifier
        # 1. Simulate existing refined TV series: Fbi S01E06 in Drive
        fbi_dir = os.path.join(config.REFINED_TV_DIR, "Fbi", "Season 01")
        os.makedirs(fbi_dir, exist_ok=True)
        fbi_refined_file = os.path.join(fbi_dir, "Fbi S01E06.mkv")
        with open(fbi_refined_file, "wb") as f:
            f.write(b"fbi_s01e06_bytes")

        metadata_manager.record_item(
            media_type="tv_series",
            name="Fbi S01E06",
            status="refined",
            refined_path=fbi_refined_file,
            show_name="Fbi",
            season=1,
            episode=6
        )

        # 2. Check that a completely different show with the exact same S01E06 is NOT reported as existing!
        lotr_info = classifier.classify_media("The Lord of the Rings The Rings of Power S01E06 - Udûn")
        exists_lotr, st_lotr, path_lotr = metadata_manager.check_media_exists(lotr_info)
        self.assertFalse(exists_lotr, f"False positive collision! LOTR matched {path_lotr}")
        self.assertEqual(st_lotr, "")
        self.assertEqual(path_lotr, "")

        # 3. Check that FBI S01E06 IS correctly identified as existing in refined state
        fbi_query_info = classifier.classify_media("Fbi S01E06")
        exists_fbi, st_fbi, path_fbi = metadata_manager.check_media_exists(fbi_query_info)
        self.assertTrue(exists_fbi)
        self.assertEqual(st_fbi, "refined")
        self.assertEqual(os.path.basename(path_fbi), "Fbi S01E06.mkv")

        # 4. Check Raw directory differentiation:
        # Place Fbi S01E07 in raw directory
        raw_fbi_file = os.path.join(config.RAW_DIR, "Fbi S01E07.mkv")
        with open(raw_fbi_file, "wb") as f:
            f.write(b"raw_fbi_s01e07_bytes")

        # Check that another show with S01E07 is NOT falsely detected as raw
        sherlock_info = classifier.classify_media("Sherlock S01E07")
        exists_sherlock, st_sh, path_sh = metadata_manager.check_media_exists(sherlock_info)
        self.assertFalse(exists_sherlock, f"False positive raw collision! Sherlock matched {path_sh}")

        # Check that Fbi S01E07 IS detected as raw
        fbi_e7_info = classifier.classify_media("Fbi S01E07")
        exists_fbi_raw, st_fr, path_fr = metadata_manager.check_media_exists(fbi_e7_info)
        self.assertTrue(exists_fbi_raw)
        self.assertEqual(st_fr, "raw")
        self.assertEqual(os.path.basename(path_fr), "Fbi S01E07.mkv")

    def test_classifier_movie_formats(self):
        cases = [
            ("Inception (2010)", "Inception", 2010, "Inception (2010)"),
            ("Avatar [2009]", "Avatar", 2009, "Avatar (2009)"),
            ("The Matrix (1999) 1080p BluRay.mkv", "The Matrix", 1999, "The Matrix (1999)"),
            ("[YTS.MX] Interstellar (2014) [1080p] [WEBRip]", "Interstellar", 2014, "Interstellar (2014)"),
            ("Blade Runner 2049 (2017)", "Blade Runner 2049", 2017, "Blade Runner 2049 (2017)"),
            ("Blade.Runner.2049.2017.1080p.BluRay.x264", "Blade Runner 2049", 2017, "Blade Runner 2049 (2017)"),
            ("2001 A Space Odyssey 1968 1080p BluRay", "2001 A Space Odyssey", 1968, "2001 A Space Odyssey (1968)"),
            ("2012 2009 1080p BluRay", "2012", 2009, "2012 (2009)"),
            ("Gladiator.II.2024.HDRip.XviD-EVO", "Gladiator Ii", 2024, "Gladiator Ii (2024)"),
            ("Pulp.Fiction.1080p.BluRay.x264.mkv", "Pulp Fiction", None, "Pulp Fiction"),
            ("Fight.Club.720p", "Fight Club", None, "Fight Club")
        ]
        for raw_input, expected_title, expected_year, expected_fmt in cases:
            res = classifier.heuristic_classify(raw_input)
            self.assertEqual(res["type"], "movie", f"Failed movie type for {raw_input}")
            self.assertEqual(res["year"], expected_year, f"Failed year for {raw_input}")
            self.assertEqual(res["title"], expected_title, f"Failed title for {raw_input}")
            self.assertEqual(res["formatted_name"], expected_fmt, f"Failed formatted_name for {raw_input}")

    def test_movie_deduplication_and_catalog(self):
        # 1. Test _is_movie_match precision
        self.assertTrue(metadata_manager._is_movie_match("Inception (2010)", None, "Inception", 2010))
        self.assertTrue(metadata_manager._is_movie_match("The Matrix", 1999, "Matrix, The (1999)", None))
        # Different years must NOT match
        self.assertFalse(metadata_manager._is_movie_match("Avatar (2009)", None, "Avatar (2022)", None))
        self.assertFalse(metadata_manager._is_movie_match("Dune (1984)", 1984, "Dune (2021)", 2021))

        # 2. Test record_item updates existing without duplicating
        metadata_manager.record_item(
            media_type="movie",
            name="Inception (2010)",
            status="raw",
            raw_path=os.path.join(config.RAW_DIR, "Inception (2010).mkv"),
            year=2010
        )
        data = metadata_manager.load_metadata()
        self.assertEqual(len(data["movies"]), 1)
        self.assertEqual(data["movies"][0]["status"], "raw")
        self.assertEqual(data["movies"][0]["year"], 2010)

        # Update status using update_item_status
        metadata_manager.update_item_status(
            media_type="movie",
            name="Inception",
            status="refined",
            refined_path=os.path.join(config.REFINED_MOVIES_DIR, "Inception (2010).mkv"),
            year=2010
        )
        data2 = metadata_manager.load_metadata()
        self.assertEqual(len(data2["movies"]), 1)
        self.assertEqual(data2["movies"][0]["status"], "refined")

        # 3. Test check_media_exists with raw scene filename in RAW_DIR
        scene_raw_file = os.path.join(config.RAW_DIR, "Avatar.2009.720p.BluRay.x264.mkv")
        with open(scene_raw_file, "wb") as f:
            f.write(b"avatar_raw_bytes")

        query_info = classifier.classify_media("Avatar (2009)")
        exists, st, p = metadata_manager.check_media_exists(query_info)
        self.assertTrue(exists)
        self.assertEqual(st, "raw")
        self.assertEqual(os.path.basename(p), "Avatar.2009.720p.BluRay.x264.mkv")

        # 4. Test render_catalog_html sorting for movies
        metadata_manager.record_item(media_type="movie", name="Zack Snyder Justice League (2021)", status="refined", year=2021)
        metadata_manager.record_item(media_type="movie", name="Avatar (2009)", status="refined", year=2009)
        metadata_manager.record_item(media_type="movie", name="Avatar: The Way of Water (2022)", status="refined", year=2022)

        html = ui.render_catalog_html()
        idx_avatar1 = html.find("Avatar (2009)")
        idx_avatar2 = html.find("Avatar: The Way of Water (2022)")
        idx_justice = html.find("Zack Snyder Justice League (2021)")
        self.assertTrue(idx_avatar1 != -1 and idx_avatar2 != -1 and idx_justice != -1)
        self.assertTrue(idx_avatar1 < idx_avatar2 < idx_justice, "Movies are not sorted alphabetically in Media Catalog!")

    def test_tunnel_module_and_banner(self):
        import tunnel
        # 1. Test display_access_banner output formatting without error
        tunnel.display_access_banner(
            port=7860,
            cloudflare_url="https://test-subdomain.trycloudflare.com",
            gradio_share_url="https://test-share.gradio.live"
        )
        # 2. Test safe_print handles unicode safely
        tunnel.safe_print("Test Unicode: \U0001f310 \U0001f680 \u2705")

        # 3. Test setup_colab_access doesn't crash in non-colab environments
        tunnel.setup_colab_access(7860)

    def test_cluster_database_operations(self):
        # 1. Test worker registration
        w_data = {
            "worker_id": "worker_test_t4_1",
            "node_name": "Test Node 1",
            "gpu_name": "Tesla T4",
            "vram_mb": 15360,
            "tunnel_url": "https://test1.trycloudflare.com"
        }
        ok = database.upsert_cluster_worker(w_data)
        self.assertTrue(ok)

        # 2. Verify worker in database
        workers = database.get_cluster_workers()
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["worker_id"], "worker_test_t4_1")
        self.assertEqual(workers[0]["gpu_name"], "Tesla T4")
        self.assertEqual(workers[0]["status"], "idle")

        # 3. Test heartbeat and progress update
        ok_hb = database.heartbeat_cluster_worker("worker_test_t4_1", {
            "job_id": 10,
            "chunk_idx": 2,
            "pct": 45.0,
            "fps": 180.0,
            "speed": "14.5x",
            "status": "encoding"
        })
        self.assertTrue(ok_hb)
        w_updated = database.get_cluster_workers()[0]
        self.assertEqual(w_updated["status"], "encoding")
        self.assertEqual(w_updated["current_progress"], 45.0)

        # 4. Test chunk increment
        database.increment_worker_chunks("worker_test_t4_1")
        w_done = database.get_cluster_workers()[0]
        self.assertEqual(w_done["total_chunks_processed"], 1)
        self.assertEqual(w_done["status"], "idle")

        # 5. Test stale worker detection
        with database._db_lock:
            conn = database._get_connection()
            conn.execute("UPDATE cluster_workers SET last_heartbeat = ?", (time.time() - 100,))
            conn.commit()
            conn.close()

        workers_stale = database.get_cluster_workers(timeout_sec=45.0)
        self.assertEqual(workers_stale[0]["status"], "offline")

        # 6. Test cleanup stale workers
        cleaned = database.cleanup_stale_cluster_workers(timeout_sec=50.0)
        self.assertEqual(cleaned, 1)
        self.assertEqual(len(database.get_cluster_workers()), 0)

    def test_cluster_manager_beacons_and_discovery(self):
        import cluster_manager

        # 1. Test master beacon publishing
        cluster_manager.publish_master_beacon("https://master-coord.trycloudflare.com")
        self.assertTrue(os.path.exists(config.CLUSTER_MASTER_FILE))
        with open(config.CLUSTER_MASTER_FILE, "r") as f:
            data = json.load(f)
        self.assertEqual(data["master_url"], "https://master-coord.trycloudflare.com")

        # 2. Test worker drive beacon discovery
        worker_file = os.path.join(config.CLUSTER_WORKERS_DIR, "worker_drive_test.json")
        with open(worker_file, "w") as f:
            json.dump({
                "worker_id": "worker_drive_test",
                "node_name": "Drive Helper",
                "gpu_name": "Tesla A100",
                "vram_mb": 40960,
                "status": "idle",
                "last_heartbeat": time.time(),
                "registered_at": time.time()
            }, f)

        cluster_manager.discover_workers_from_drive()
        workers = database.get_cluster_workers()
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["worker_id"], "worker_drive_test")
        self.assertEqual(workers[0]["gpu_name"], "Tesla A100")

    def test_cluster_manager_task_leasing_and_reclaim(self):
        import cluster_manager

        # Setup mock active cluster job
        raw_seg_0 = os.path.join(self.test_dir, "seg_000.mkv")
        out_seg_0 = os.path.join(self.test_dir, "enc_seg_000.mkv")
        with open(raw_seg_0, "wb") as f:
            f.write(b"dummy_chunk")

        job_info = {
            "job_id": 99,
            "title": "Test Movie",
            "chunks_dir": self.test_dir,
            "raw_chunks": [raw_seg_0],
            "chunks": {
                0: {
                    "raw_path": raw_seg_0,
                    "out_path": out_seg_0,
                    "status": "pending",
                    "worker_id": None,
                    "leased_until": 0.0,
                    "pct": 0.0,
                    "fps": 0.0,
                    "speed": "--"
                }
            },
            "refining_config": {"res_preset": "480p"},
            "cancelled": False
        }
        cluster_manager.register_cluster_job(99, job_info)

        # 1. Claim task
        task = cluster_manager.claim_task("worker_node_1")
        self.assertTrue(task["has_task"])
        self.assertEqual(task["job_id"], 99)
        self.assertEqual(task["chunk_idx"], 0)
        self.assertEqual(job_info["chunks"][0]["status"], "leased")
        self.assertEqual(job_info["chunks"][0]["worker_id"], "worker_node_1")

        # 2. Subsequent claim has no tasks
        no_task = cluster_manager.claim_task("worker_node_2")
        self.assertFalse(no_task["has_task"])

        # 3. Simulate lease expiration and reclaim
        job_info["chunks"][0]["leased_until"] = time.time() - 10
        cluster_manager.reclaim_stale_chunks()
        self.assertEqual(job_info["chunks"][0]["status"], "pending")
        self.assertIsNone(job_info["chunks"][0]["worker_id"])

        # 4. Re-claim and complete
        task_reclaimed = cluster_manager.claim_task("worker_node_2")
        self.assertTrue(task_reclaimed["has_task"])
        self.assertEqual(task_reclaimed["chunk_idx"], 0)

        # Complete task
        with open(out_seg_0, "wb") as f:
            f.write(b"encoded_chunk")
        res_comp = cluster_manager.complete_task("worker_node_2", 99, 0, out_seg_0, 1.5)
        self.assertEqual(res_comp["status"], "accepted")
        self.assertTrue(res_comp["all_chunks_done"])
        self.assertEqual(job_info["chunks"][0]["status"], "done")

        cluster_manager.unregister_cluster_job(99)

    def test_cluster_ui_rendering(self):
        import cluster_manager
        # Render with no workers
        html_empty = ui.render_cluster_html()
        self.assertIn("No Remote GPU Workers Connected Yet", html_empty)

        # Add worker and render
        database.upsert_cluster_worker({
            "worker_id": "worker_ui_t4",
            "node_name": "Cluster Node 1",
            "gpu_name": "Tesla T4",
            "vram_mb": 15360,
            "status": "idle"
        })
        html_worker = ui.render_cluster_html()
        self.assertIn("Cluster Node 1", html_worker)
        self.assertIn("Tesla T4", html_worker)
        self.assertIn("15.0 GB", html_worker)

    def test_helper_worker_module(self):
        import helper_worker

        worker = helper_worker.HelperWorker(worker_id="test_worker_standalone")
        self.assertEqual(worker.worker_id, "test_worker_standalone")

        # Test Drive beacon announcement
        worker.announce_to_drive(status="idle")
        w_file = os.path.join(config.CLUSTER_WORKERS_DIR, "worker_test_worker_standalone.json")
        self.assertTrue(os.path.exists(w_file))
        with open(w_file, "r") as f:
            d = json.load(f)
        self.assertEqual(d["worker_id"], "test_worker_standalone")
        self.assertEqual(d["status"], "idle")

        # Test master beacon discovery
        cluster_manager_target = config.CLUSTER_MASTER_FILE
        os.makedirs(os.path.dirname(cluster_manager_target), exist_ok=True)
        with open(cluster_manager_target, "w") as f:
            json.dump({"master_url": "https://test-main-coord.trycloudflare.com"}, f)

        discovered_url = worker.discover_master_url()
        self.assertEqual(discovered_url, "https://test-main-coord.trycloudflare.com")

if __name__ == "__main__":
    unittest.main()
