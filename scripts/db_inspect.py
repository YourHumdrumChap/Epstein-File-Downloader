from __future__ import annotations

import sqlite3

from doj_disclosures.core.config import AppConfig


def main() -> None:
    cfg = AppConfig.load()
    print("db:", cfg.paths.db_path)

    conn = sqlite3.connect(cfg.paths.db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*), COALESCE(content_type,'') "
            "FROM urls WHERE status='done' "
            "GROUP BY COALESCE(content_type,'') "
            "ORDER BY COUNT(*) DESC"
        )
        rows = cur.fetchall()
        print("done by content_type (top 10):")
        for r in rows[:10]:
            print(" ", r)

        cur.execute(
            "SELECT url,status,COALESCE(http_status,0),COALESCE(content_type,''),"
            "SUBSTR(COALESCE(error,''),1,80),COALESCE(sha256,''),COALESCE(local_path,'') "
            "FROM urls WHERE status='done' ORDER BY discovered_at DESC LIMIT 10"
        )
        print("recent done:")
        for r in cur.fetchall():
            print(" ", r)

        cur.execute("SELECT id,url,sha256,local_path FROM documents ORDER BY id DESC LIMIT 10")
        print("documents:")
        for r in cur.fetchall():
            print(" ", r)

        cur.execute("SELECT COUNT(*) FROM documents")
        print("documents_count:", cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM matches")
        print("matches_count:", cur.fetchone()[0])

        cur.execute("SELECT status, COUNT(*) FROM urls WHERE url LIKE '%.pdf' GROUP BY status ORDER BY COUNT(*) DESC")
        print("pdf urls by status:")
        for r in cur.fetchall():
            print(" ", r)

        cur.execute(
            "SELECT "
            "SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done_cnt, "
            "SUM(CASE WHEN status='done' AND (local_path IS NULL OR local_path='') THEN 1 ELSE 0 END) AS done_no_path, "
            "SUM(CASE WHEN status='done' AND (sha256 IS NULL OR sha256='') THEN 1 ELSE 0 END) AS done_no_sha, "
            "SUM(CASE WHEN status='done' AND COALESCE(content_type,'')='' THEN 1 ELSE 0 END) AS done_no_ct, "
            "SUM(CASE WHEN status='done' AND COALESCE(http_status,0)=0 THEN 1 ELSE 0 END) AS done_no_http "
            "FROM urls WHERE url LIKE '%.pdf'"
        )
        done_cnt, done_no_path, done_no_sha, done_no_ct, done_no_http = cur.fetchone()
        print(
            "pdf done detail:",
            {
                "done": done_cnt,
                "done_no_local_path": done_no_path,
                "done_no_sha256": done_no_sha,
                "done_no_content_type": done_no_ct,
                "done_no_http_status": done_no_http,
            },
        )

        cur.execute(
            "SELECT url,status,COALESCE(http_status,0),COALESCE(content_type,''),"
            "SUBSTR(COALESCE(error,''),1,120),COALESCE(sha256,''),COALESCE(local_path,'') "
            "FROM urls WHERE url LIKE '%.pdf' AND status='done' "
            "ORDER BY discovered_at DESC LIMIT 15"
        )
        print("recent done pdf rows:")
        for r in cur.fetchall():
            print(" ", r)

        cur.execute("SELECT COUNT(*) FROM urls WHERE url LIKE '%.pdf'")
        print("pdf_url_count:", cur.fetchone()[0])

    finally:
        conn.close()


if __name__ == "__main__":
    main()
