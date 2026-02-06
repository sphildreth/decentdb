import gzip
import os

import pytest

import decentdb
from decentdb.tools.pgbak_import import convert_pg_dump_to_decentdb, write_report_json


def _make_pg_dump_source(path: str) -> None:
    """Create a sample PostgreSQL dump file."""
    dump_content = """--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: Artists; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public."Artists" (
    "Id" integer NOT NULL,
    "Name" character varying(255) NOT NULL,
    "Bio" text,
    "Active" boolean NOT NULL,
    "Rating" double precision
);


ALTER TABLE public."Artists" OWNER TO postgres;

--
-- Name: Albums; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public."Albums" (
    "Id" integer NOT NULL,
    "ArtistId" integer NOT NULL,
    "Title" character varying(255) NOT NULL,
    "ReleaseYear" integer,
    "Price" numeric(10,2)
);


ALTER TABLE public."Albums" OWNER TO postgres;

--
-- Name: Songs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public."Songs" (
    "Id" integer NOT NULL,
    "AlbumId" integer,
    "Title" character varying(255) NOT NULL,
    "Duration" double precision
);


ALTER TABLE public."Songs" OWNER TO postgres;

--
-- Data for Name: Artists; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public."Artists" ("Id", "Name", "Bio", "Active", "Rating") FROM stdin;
1	The Beatles	Legendary rock band from Liverpool	t	9.5
2	Pink Floyd	Progressive rock pioneers	t	9.2
3	Led Zeppelin	Hard rock gods	f	9.0
\\.


--
-- Data for Name: Albums; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public."Albums" ("Id", "ArtistId", "Title", "ReleaseYear", "Price") FROM stdin;
1	1	Abbey Road	1969	19.99
2	1	Sgt. Pepper's	1967	18.99
3	2	The Dark Side of the Moon	1973	21.99
4	3	Led Zeppelin IV	1971	17.99
\\.


--
-- Data for Name: Songs; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public."Songs" ("Id", "AlbumId", "Title", "Duration") FROM stdin;
1	1	Come Together	260.5
2	1	Something	183.2
3	2	Lucy in the Sky	208.4
4	3	Time	413.2
5	4	Stairway to Heaven	482.1
\\.


--
-- Name: Artists Artists_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public."Artists"
    ADD CONSTRAINT "Artists_pkey" PRIMARY KEY ("Id");


--
-- Name: Albums Albums_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public."Albums"
    ADD CONSTRAINT "Albums_pkey" PRIMARY KEY ("Id");


--
-- Name: Songs Songs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public."Songs"
    ADD CONSTRAINT "Songs_pkey" PRIMARY KEY ("Id");


--
-- Name: Albums Albums_ArtistId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public."Albums"
    ADD CONSTRAINT "Albums_ArtistId_fkey" FOREIGN KEY ("ArtistId") REFERENCES public."Artists"("Id");


--
-- Name: Songs Songs_AlbumId_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public."Songs"
    ADD CONSTRAINT "Songs_AlbumId_fkey" FOREIGN KEY ("AlbumId") REFERENCES public."Albums"("Id") ON DELETE SET NULL;


--
-- Name: IX_Albums_ArtistId; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "IX_Albums_ArtistId" ON public."Albums" USING btree ("ArtistId");


--
-- Name: IX_Songs_AlbumId; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "IX_Songs_AlbumId" ON public."Songs" USING btree ("AlbumId");


--
-- PostgreSQL database dump complete
--
"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(dump_content)


def _make_pg_dump_gzipped(path: str) -> None:
    """Create a gzipped PostgreSQL dump file."""
    plain_path = path.replace(".gz", "")
    _make_pg_dump_source(plain_path)

    with open(plain_path, "rb") as f_in:
        with gzip.open(path, "wb") as f_out:
            f_out.write(f_in.read())

    os.remove(plain_path)


def test_pg_dump_to_decentdb_convert_plain(tmp_path):
    """Test converting a plain SQL dump file."""
    pg_path = str(tmp_path / "dump.sql")
    decent_path = str(tmp_path / "dst.decentdb")

    _make_pg_dump_source(pg_path)

    report = convert_pg_dump_to_decentdb(
        pg_dump_path=pg_path,
        decentdb_path=decent_path,
        overwrite=False,
        show_progress=False,
    )

    assert report.pg_dump_path == pg_path
    assert report.decentdb_path == decent_path
    assert set(report.tables) == {"artists", "albums", "songs"}
    assert report.rows_copied.get("artists") == 3
    assert report.rows_copied.get("albums") == 4
    assert report.rows_copied.get("songs") == 5

    # Check JSON report
    report_path = str(tmp_path / "report.json")
    write_report_json(report, report_path)
    assert (tmp_path / "report.json").exists()

    # Verify database contents
    conn = decentdb.connect(decent_path)
    try:
        # Check tables exist
        listed = conn.list_tables()
        if listed and isinstance(listed[0], dict):
            names = {t["name"] for t in listed}
        else:
            names = {str(t) for t in listed}
        assert {"artists", "albums", "songs"}.issubset(names)

        # Check row counts
        assert conn.execute('SELECT COUNT(*) FROM "artists"').fetchone()[0] == 3
        assert conn.execute('SELECT COUNT(*) FROM "albums"').fetchone()[0] == 4
        assert conn.execute('SELECT COUNT(*) FROM "songs"').fetchone()[0] == 5

        # Check data integrity (column names are lowercased by default)
        artists = conn.execute(
            'SELECT "id", "name", "active" FROM "artists" ORDER BY "id"'
        ).fetchall()
        assert artists[0] == (1, "The Beatles", True)
        assert artists[1] == (2, "Pink Floyd", True)
        assert artists[2] == (3, "Led Zeppelin", False)

        # Check foreign key enforcement
        with pytest.raises(decentdb.IntegrityError):
            conn.execute(
                'INSERT INTO "albums" ("id", "artistid", "title") VALUES (?, ?, ?)',
                (999, 999, "Test Album"),
            )

    finally:
        conn.close()


def test_pg_dump_to_decentdb_convert_gzipped(tmp_path):
    """Test converting a gzipped SQL dump file."""
    pg_path = str(tmp_path / "dump.sql.gz")
    decent_path = str(tmp_path / "dst.decentdb")

    _make_pg_dump_gzipped(pg_path)

    report = convert_pg_dump_to_decentdb(
        pg_dump_path=pg_path,
        decentdb_path=decent_path,
        overwrite=False,
        show_progress=False,
    )

    assert report.pg_dump_path == pg_path
    assert set(report.tables) == {"artists", "albums", "songs"}
    assert report.rows_copied.get("artists") == 3

    conn = decentdb.connect(decent_path)
    try:
        assert conn.execute('SELECT COUNT(*) FROM "artists"').fetchone()[0] == 3
    finally:
        conn.close()


def test_pg_dump_to_decentdb_convert_chunked_commits(tmp_path):
    """Test with chunked commits."""
    pg_path = str(tmp_path / "dump.sql")
    decent_path = str(tmp_path / "dst.decentdb")

    _make_pg_dump_source(pg_path)

    report = convert_pg_dump_to_decentdb(
        pg_dump_path=pg_path,
        decentdb_path=decent_path,
        overwrite=False,
        show_progress=False,
        commit_every=1,
    )

    assert report.rows_copied.get("artists") == 3
    assert report.rows_copied.get("albums") == 4
    assert report.rows_copied.get("songs") == 5

    conn = decentdb.connect(decent_path)
    try:
        assert conn.execute('SELECT COUNT(*) FROM "artists"').fetchone()[0] == 3
    finally:
        conn.close()


def test_pg_dump_identifier_case_preserve(tmp_path):
    """Test preserving identifier case."""
    pg_path = str(tmp_path / "dump.sql")
    decent_path = str(tmp_path / "dst.decentdb")

    _make_pg_dump_source(pg_path)

    report = convert_pg_dump_to_decentdb(
        pg_dump_path=pg_path,
        decentdb_path=decent_path,
        overwrite=False,
        show_progress=False,
        identifier_case="preserve",
    )

    # Check that original case is preserved
    assert "Artists" in report.tables or any(
        t.lower() == "artists" for t in report.tables
    )


def test_pg_dump_overwrite_protection(tmp_path):
    """Test that overwrite protection works."""
    pg_path = str(tmp_path / "dump.sql")
    decent_path = str(tmp_path / "dst.decentdb")

    _make_pg_dump_source(pg_path)

    # First conversion
    convert_pg_dump_to_decentdb(
        pg_dump_path=pg_path,
        decentdb_path=decent_path,
        overwrite=False,
        show_progress=False,
    )

    # Second conversion without overwrite should fail
    from decentdb.tools.pgbak_import import ConversionError

    with pytest.raises(ConversionError):
        convert_pg_dump_to_decentdb(
            pg_dump_path=pg_path,
            decentdb_path=decent_path,
            overwrite=False,
            show_progress=False,
        )

    # Second conversion with overwrite should succeed
    convert_pg_dump_to_decentdb(
        pg_dump_path=pg_path,
        decentdb_path=decent_path,
        overwrite=True,
        show_progress=False,
    )


def test_pg_dump_file_not_found():
    """Test handling of missing file."""
    from decentdb.tools.pgbak_import import ConversionError

    with pytest.raises(FileNotFoundError):
        convert_pg_dump_to_decentdb(
            pg_dump_path="/nonexistent/path/dump.sql",
            decentdb_path="/tmp/test.db",
            show_progress=False,
        )


def _make_pg_dump_nullable_numerics(path: str) -> None:
    """Create a PG dump with nullable int4 and float8 columns containing NULLs."""
    dump = """\
SET default_table_access_method = heap;

CREATE TABLE public."Tracks" (
    "Id" integer NOT NULL,
    "Title" character varying(255) NOT NULL,
    "ImageCount" int4 NULL,
    "ReplayGain" float8 NULL,
    "DeezerId" int4 NULL
);

COPY public."Tracks" ("Id", "Title", "ImageCount", "ReplayGain", "DeezerId") FROM stdin;
1	Hello	5	-3.14	1001
2	World	\\N	\\N	\\N
3	Test	0	0.0	2002
\\.

ALTER TABLE ONLY public."Tracks"
    ADD CONSTRAINT "Tracks_pkey" PRIMARY KEY ("Id");
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(dump)


def test_pg_dump_nullable_int4_not_downgraded_to_text(tmp_path):
    """int4 NULL columns with NULL values must import as INT64, not TEXT."""
    pg_path = str(tmp_path / "dump.sql")
    decent_path = str(tmp_path / "dst.decentdb")

    _make_pg_dump_nullable_numerics(pg_path)

    report = convert_pg_dump_to_decentdb(
        pg_dump_path=pg_path,
        decentdb_path=decent_path,
        overwrite=False,
        show_progress=False,
    )

    # No warnings about non-numeric data for int4/float8 columns
    numeric_warnings = [w for w in report.warnings if "non-numeric" in w]
    assert numeric_warnings == [], f"Unexpected numeric warnings: {numeric_warnings}"

    conn = decentdb.connect(decent_path)
    try:
        rows = conn.execute(
            'SELECT "id", "imagecount", "replaygain", "deezerid" '
            'FROM "tracks" ORDER BY "id"'
        ).fetchall()
        assert rows[0] == (1, 5, -3.14, 1001)
        assert rows[1] == (2, None, None, None)
        assert rows[2] == (3, 0, 0.0, 2002)
    finally:
        conn.close()


def _make_pg_dump_composite_pk(path: str) -> None:
    """Create a PG dump with composite primary key tables."""
    dump = """\
SET default_table_access_method = heap;

CREATE TABLE public."Playlists" (
    "Id" integer NOT NULL,
    "Name" character varying(255) NOT NULL
);

CREATE TABLE public."Songs" (
    "Id" integer NOT NULL,
    "Title" character varying(255) NOT NULL
);

CREATE TABLE public."PlaylistSong" (
    "PlaylistId" integer NOT NULL,
    "SongId" integer NOT NULL,
    "SortOrder" integer NOT NULL
);

COPY public."Playlists" ("Id", "Name") FROM stdin;
1	Favorites
2	Chill
\\.

COPY public."Songs" ("Id", "Title") FROM stdin;
1	Song A
2	Song B
3	Song C
\\.

COPY public."PlaylistSong" ("PlaylistId", "SongId", "SortOrder") FROM stdin;
1	1	1
1	2	2
2	2	1
2	3	2
\\.

ALTER TABLE ONLY public."Playlists"
    ADD CONSTRAINT "Playlists_pkey" PRIMARY KEY ("Id");

ALTER TABLE ONLY public."Songs"
    ADD CONSTRAINT "Songs_pkey" PRIMARY KEY ("Id");

ALTER TABLE ONLY public."PlaylistSong"
    ADD CONSTRAINT "PlaylistSong_pkey" PRIMARY KEY ("PlaylistId", "SongId");
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(dump)


def test_pg_dump_composite_pk_tables_imported(tmp_path):
    """Tables with composite PKs should be imported (not skipped)."""
    pg_path = str(tmp_path / "dump.sql")
    decent_path = str(tmp_path / "dst.decentdb")

    _make_pg_dump_composite_pk(pg_path)

    report = convert_pg_dump_to_decentdb(
        pg_dump_path=pg_path,
        decentdb_path=decent_path,
        overwrite=False,
        show_progress=False,
    )

    # PlaylistSong should NOT be in skipped tables
    assert "PlaylistSong" not in report.skipped_tables
    assert "playlistsong" in report.tables

    # All data should be imported
    assert report.rows_copied.get("playlistsong") == 4

    # Should have a warning about composite PK
    composite_warnings = [w for w in report.warnings if "composite primary key" in w]
    assert len(composite_warnings) == 1

    conn = decentdb.connect(decent_path)
    try:
        rows = conn.execute(
            'SELECT "playlistid", "songid", "sortorder" '
            'FROM "playlistsong" ORDER BY "playlistid", "songid"'
        ).fetchall()
        assert len(rows) == 4
        assert rows[0] == (1, 1, 1)
        assert rows[1] == (1, 2, 2)
        assert rows[2] == (2, 2, 1)
        assert rows[3] == (2, 3, 2)
    finally:
        conn.close()
