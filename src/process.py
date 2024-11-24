import xml.etree.ElementTree as ET
from lxml import etree
import pandas as pd


def parse_dblp(xml_file):
    parser = etree.XMLParser(recover=True)  # use parser to automatically process the xml entity
    tree = etree.parse(xml_file, parser)
    root = tree.getroot()

    data = []

    for elem in root:
        entry = {}
        entry['type'] = elem.tag
        entry['key'] = elem.get('key', '')

        for child in elem:
            if child.tag in ['author', 'editor']:
                entry.setdefault('authors', []).append(child.text if child.text is not None else '')
            else:
                entry[child.tag] = child.text if child.text is not None else ''

        if 'authors' in entry:
            entry['authors'] = ', '.join([author for author in entry['authors'] if author])

        entry['title'] = entry.get('title', '')
        entry['year'] = entry.get('year', '')
        entry['booktitle'] = entry.get('booktitle', '')
        entry['ee'] = entry.get('ee', '')

        data.append(entry)

    df = pd.DataFrame(data)
    return df


# 主程序
if __name__ == "__main__":
    xml_file = "../data/dblp.xml"
    df = parse_dblp(xml_file)
    df.to_csv("dblp_extracted_data.csv", index=False)
