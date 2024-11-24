import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
from sqlalchemy.sql import text

# create mysql database connection
engine = create_engine("mysql+pymysql://root:zhaoyong987@localhost/dblp_data")


# import data into authors table
def import_authors(csv_file, batch_size=10000):
    print("Importing authors...")
    authors_set = set()

    # read data in batchs
    for chunk in tqdm(pd.read_csv(csv_file, usecols=['authors'], chunksize=batch_size), desc="Processing authors"):
        for authors in chunk['authors'].dropna():
            authors_set.update(authors.split(', '))

    authors_df = pd.DataFrame({'name': list(authors_set)})
    authors_df.to_sql('authors', con=engine, if_exists='append', index=False)


# import data into papers table
def import_papers(csv_file, batch_size=10000):
    print("Importing papers...")
    for chunk in tqdm(pd.read_csv(csv_file, usecols=['title', 'year', 'booktitle', 'url', 'ee'], chunksize=batch_size),
                      desc="Processing papers"):
        chunk.to_sql('papers', con=engine, if_exists='append', index=False)


# lead author_and_paper data into memory
def load_author_and_paper_ids():
    with engine.connect() as conn:
        authors = pd.read_sql("SELECT author_id, name FROM authors", conn)
        papers = pd.read_sql("SELECT paper_id, title FROM papers", conn)

    print("Building author dictionary...")
    author_dict = {name: author_id for author_id, name in
                   tqdm(zip(authors['author_id'], authors['name']), total=len(authors), desc="Authors")}

    print("Building paper dictionary...")
    paper_dict = {row['title']: row['paper_id'] for row in tqdm(papers.to_dict('records'), desc="Papers")}

    return author_dict, paper_dict



# import data into author_paper table
def import_author_paper_batch(csv_file, batch_size=10000):
    print("Importing author-paper relationships in batch mode...")

    author_dict, paper_dict = load_author_and_paper_ids()

    relationships = []

    for chunk in tqdm(pd.read_csv(csv_file, usecols=['authors', 'title'], chunksize=batch_size),
                      desc="Processing chunks"):
        valid_rows = chunk.dropna(subset=['authors', 'title'])
        for _, row in valid_rows.iterrows():
            title = row['title']
            if title not in paper_dict:
                print(f"Paper not found: {title}")
                continue

            paper_id = paper_dict[title]
            authors = row['authors'].split(', ')
            for author in authors:
                if author not in author_dict:
                    print(f"Author not found: {author}")
                    continue

                author_id = author_dict[author]
                relationships.append((author_id, paper_id))

        if relationships:
            with engine.connect() as conn:
                conn.execute(
                    text("INSERT IGNORE INTO author_paper (author_id, paper_id) VALUES (:author_id, :paper_id)"),
                    [{"author_id": rel[0], "paper_id": rel[1]} for rel in relationships]
                )
            relationships.clear()


if __name__ == "__main__":
    csv_file = '../data/dblp_extracted_data.csv'

    import_authors(csv_file)
    import_papers(csv_file)
    import_author_paper_batch(csv_file)

    print("Data import completed!")
