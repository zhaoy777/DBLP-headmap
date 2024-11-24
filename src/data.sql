CREATE DATABASE dblp_data;

USE dblp_data;

CREATE TABLE authors (
    author_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE papers (
    paper_id INT AUTO_INCREMENT PRIMARY KEY,
    title TEXT,
    year INT,
    booktitle VARCHAR(255),
    url TEXT,
    ee TEXT
);

CREATE TABLE author_paper (
    author_id INT,
    paper_id INT,
    PRIMARY KEY (author_id, paper_id),
    FOREIGN KEY (author_id) REFERENCES authors(author_id),
    FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
);

CREATE TABLE citations (
    paper_id INT,
    cited_paper_id INT,
    PRIMARY KEY (paper_id, cited_paper_id),
    FOREIGN KEY (paper_id) REFERENCES papers(paper_id),
    FOREIGN KEY (cited_paper_id) REFERENCES papers(paper_id)
);
