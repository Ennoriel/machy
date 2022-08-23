from machy.utils.request import process_rule, get_content, USER_AGENT, getter_text
from machy.utils.file import create_file_if_does_not_exist
from machy.utils.log import LogDecorator
import pandas as pd


@LogDecorator()
def crawl_web(rules, folder_html, output_file, mode="merge", encoding="cp-1252"):
	"""
	crawl web according to the set of rules
	:param rules: set of rules to crawl the web
	:param folder_html: output folder
	:param output_file: output file
	:param mode: "append" or "merge" the csv values
	:param encoding: file encoding
	:return:
	"""
	create_file_if_does_not_exist(output_file)
	for rule in rules:
		print('  ' + rule['url'])
		for page in range(rule['page_start'], rule['page_end']):
			process_rule(rule['url'].format(page), folder_html, rule, output_file, mode, encoding)
		if 'extra_pages' in rule:
			for extra in rule['extra_pages']:
				process_rule(rule['url'].format(extra), folder_html, rule, output_file, mode, encoding)


def crawl_web_from_csv(folder_html, input_file, url_prefix, url_col, encoding="cp1252"):
	print('crawl_web')

	df = pd.read_csv(input_file)

	t = df.apply(lambda x: get_content(url_prefix + x, folder_html, "utf-8", getter_text, USER_AGENT))
	t.columns = ['content']

	print()
	print(df)
	print()
	print(t)
	print()
