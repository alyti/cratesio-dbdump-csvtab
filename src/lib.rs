use flate2::read::GzDecoder;
use std::{collections::HashMap, fs::{create_dir_all, File}, io, path::{Path, PathBuf}};
use thiserror::Error;

use cached_path::{Cache, CacheBuilder, Error as CachedError};
use rusqlite::{Connection, Error as SqliteError};

pub use cached_path;
pub use rusqlite;

#[derive(Error, Debug)]
pub enum Error {
    #[error("dump not found")]
    NotFound(#[from] CachedError),

    #[error("failed to create virtual table")]
    RusqliteError(#[from] SqliteError),

    #[error("failed to unpack dump")]
    IOError(#[from] io::Error),
}

pub struct CratesIODumpLoader {
    pub resource: String,
    pub files: Vec<PathBuf>,
    pub cache: Cache,
    pub target_path: PathBuf,

    table_schema: HashMap<String, String>,
}

impl Default for CratesIODumpLoader {
    fn default() -> Self {
        Self {
            resource: "https://static.crates.io/db-dump.tar.gz".to_string(),
            files: tables_to_files(&[
                "badges",
                "categories",
                "crate_owners",
                "crates",
                "crates_categories",
                "crates_keywords",
                "dependencies",
                "keywords",
                "metadata",
                "reserved_crate_names",
                "teams",
                "users",
                "version_authors",
                "version_downloads",
                "versions",
            ]),
            cache: Cache::new().unwrap(), // TODO: Maybe just store the builder instead... idk...
            target_path: Path::new("data").to_path_buf(),
            table_schema: HashMap::new(),
        }
    }
}

impl CratesIODumpLoader {
    pub fn resource(&mut self, path: &str) -> &mut Self {
        self.resource = path.to_owned();
        self
    }

    pub fn files(&mut self, files: Vec<PathBuf>) -> &mut Self {
        self.files = files;
        self
    }

    pub fn tables(&mut self, tables: &[&str]) -> &mut Self {
        self.files = tables_to_files(tables);
        self
    }

    pub fn table_schema(&mut self, table: &str, schema: &str) -> &mut Self {
        self.table_schema.insert(table.to_string(), schema.to_string());
        self
    }

    pub fn target_path(&mut self, path: &Path) -> &mut Self {
        self.target_path = path.to_path_buf();
        self
    }

    pub fn cache(&mut self, builder: CacheBuilder) -> Result<&mut Self, Error> {
        self.cache = builder.build()?;
        Ok(self)
    }

    pub fn minimal(&mut self) -> &mut Self {
        self.tables(&["crates", "dependencies", "versions"])
    }

    pub fn update(&mut self) -> Result<&mut Self, Error> {
        let path = self.cache.cached_path(&self.resource)?;

        let first_local_file = self.target_path.join(self.files.first().unwrap());
        if first_local_file.exists()
            && path.metadata()?.created()? <= first_local_file.metadata()?.created()?
        {
            // TODO: Improve change-detection later, this is just to prevent re-extracting existing obsurdity.
            return Ok(self);
        }

        // Extract files manually instead of letting cached_path do it so we don't have to worry about {date} folder.
        let tar_gz = File::open(path)?;
        let tar = GzDecoder::new(tar_gz);
        let mut archive = tar::Archive::new(tar);

        create_dir_all(&self.target_path)?;
        for file in archive.entries().unwrap() {
            let mut f = file.unwrap();
            let aname = match f.path().unwrap_or_default().file_name() {
                Some(p) => PathBuf::from(p),
                None => PathBuf::default(),
            };
            if self.files.contains(&aname) {
                f.unpack(self.target_path.join(aname))?;
            }
        }

        Ok(self)
    }

    pub fn create_virtual_tables(&mut self, db: &Connection) -> Result<(), Error> {
        let schema = self
            .files
            .iter()
            .map(|f| self.file_to_query(f))
            .fold(String::new(), |a, b| a + b.as_str() + "\n");
        db.execute_batch(schema.as_str())?;
        Ok(())
    }

    fn file_to_query(&self, path: &PathBuf) -> String {
        let actual_file = self.target_path.join(path);
        let table = path.file_stem().unwrap_or_default().to_string_lossy();
        match self.table_schema.get(&table.to_string()) {
            Some(schema) => format!(
                "CREATE VIRTUAL TABLE {} USING csv(filename='{}',schema='{}');",
                table,
                actual_file.display(),
                schema,
            ),
            None => format!(
                "CREATE VIRTUAL TABLE {} USING csv(filename='{}',header=yes);",
                table,
                actual_file.display(),
            )
        }
    }
}

fn tables_to_files(tables: &[&str]) -> Vec<PathBuf> {
    tables
        .iter()
        .map(|t| {
            let mut buf = PathBuf::new();
            buf.set_file_name(t);
            buf.set_extension("csv");
            buf
        })
        .collect()
}

#[test]
fn test_basic_csvtab() -> Result<(), Error> {
    // Setup cache.
    let cache = Cache::builder().progress_bar(None);

    // Setup db /w csvtab module.
    let db = Connection::open_in_memory().unwrap();
    rusqlite::vtab::csvtab::load_module(&db).unwrap();

    // Load dump from a .tar.gz archive.
    CratesIODumpLoader::default()
        .resource("testdata/test.tar.gz")
        .target_path(Path::new("testdata/extracted"))
        .tables(&["test"])
        .table_schema("test", "CREATE TABLE x(renamed_id INT, name TEXT);")
        .cache(cache)?
        .update()?
        .create_virtual_tables(&db)?;

    let mut s = db.prepare("SELECT renamed_id FROM test WHERE name = ?")?;
    let dummy = s.query_row(["awooo"], |row| row.get::<_, String>(0))?;
    assert_eq!("3", dummy);
    Ok(())
}
