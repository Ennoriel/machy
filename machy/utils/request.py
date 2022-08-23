import lxml
import pandas as pd
from os import path, makedirs
from requests import get as requests_get
from re import search, compile, MULTILINE, DOTALL
from lxml import html
import numpy as np
from json import dumps
from .log import LogDecorator


USER_AGENT = {'User-agent': 'Mozilla/5.0'}


@LogDecorator()
def process_rule(url, folder_html, rule, output_file, mode, encoding):
	"""
	1. get_html_content: get the html page content either locally if already downloaded or online and eventually saves it locally
	2. scrap_data: parse the html page content according to the rule
	3. update_csv: updates the CSV file
	:param url: url of the page to scrap
	:param folder_html: folder where the local html content pages are saved
	:param rule: scrapping rule
	:param output_file: output csv file
	:param mode: "append" or "merge" the csv values
	:param encoding: file encoding
	:return:
	"""
	html_content = get_html_content(url, folder_html, encoding)
	if html_content:
		try:
			processed_data = scrap_data(html_content, rule)
			update_csv(output_file, processed_data, mode)
		except Exception as e:
			if rule["continue_if_error"]:
				print('       silent error', e)
			else:
				raise e


@LogDecorator()
def process_rule_2(url, rule, folder_html, encoding):
	print('    ' + url)
	html_content = get_html_content(url, folder_html, encoding)
	if html_content:
		return scrap_data(html_content, rule)


def get_html_content(url, folder, encoding, headers=USER_AGENT):
	return get_content(url, folder, encoding, getter_html, headers)


def get_json_content(url, folder, encoding, headers=USER_AGENT):
	return get_content(url, folder, encoding, getter_json, headers)


def getter_html(response, encoding):
	return response.content.decode(encoding=encoding, errors="ignore")


def getter_json(response, encoding):
	return dumps(response.json())


def getter_text(response, encoding):
	return response.text


def get_content(url, folder_html, encoding, getter, headers):
	"""
	get the html page content either locally if already downloaded or online and eventually saves it locally
	:param url: url of the page to scrap
	:param folder_html: folder where the local html content pages are saved
	:param encoding: file encoding
	:param getter: method to parse the file (if it is json / html / other)
	:param headers: request headers
	:return: html page content
	"""
	local_file = path.join(folder_html, url.replace('/', '-').replace('?', '-').replace(':', '-') + '.html')

	if not path.isdir(folder_html):
		makedirs(folder_html)

	if path.isfile(local_file + '.err'):
		return None

	if path.isfile(local_file):
		with open(local_file, "r", encoding=encoding) as f:
			return f.read()

	else:
		page = requests_get(url, headers=headers)
		if page.status_code == 200:
			print('   file successfully downloaded')
			try:
				with open(local_file, 'w', encoding=encoding) as f:
					print('!!!!!!!!!!!!!!!!!!!!!!! ' + encoding)
					f.write(getter(page, encoding))
			except IOError:
				print('problem while writing file {}'.format(local_file))
		else:
			print('Unexpected error: HTTP code {} at url {}'.format(page.status_code, url))
			print(page.headers)
			with open(local_file + '.err', 'w') as f:
				f.write(getter(page, encoding))
			return None

	return getter(page, encoding)


def scrap_data(html_content, rule):
	"""
	parse the html page content according to the rule
	:param html_content: html content
	:param rule: scrapping rule
	:return: dataFrame with the scrapped data
	"""
	html_lxml = html.fromstring(html_content)

	df = pd.DataFrame()
	columns = ['id', ] + [e['label'] for e in rule['data']]

	datas = []

	if rule['is_list_of_items']:
		for item_nb in range(rule['item_start'], rule['item_end']):
			id_xpath = get_xpath(rule['id']['xpath'], rule['id']['xpath_param'], item_nb)
			v = get_xpath_regex_content(html_lxml, id_xpath, rule, item_nb)
			print(v)
			if v:
				datas.append(v)
	else:
		id_xpath = rule['id']['xpath']
		datas.append(get_xpath_regex_content(html_lxml, id_xpath, rule))

	df = df.append(pd.DataFrame(datas, columns=columns))

	return df


def update_csv(file, processed_data, mode):
	"""
	updates the CSV file
	:param file: CSV file
	:param processed_data: dataframe to save
	:param mode: "append" or "merge" the csv values
	"""
	# Read saved datasset
	try:
		saved_data = pd.read_csv(file)
		saved_data = saved_data.astype({'id': np.str})
	except (pd.errors.EmptyDataError, FileNotFoundError):
		processed_data.to_csv(file, index=False)
		return

	# Initialize empty column if a new column is added
	for column in [column for column in processed_data.columns if column != 'id']:
		if column not in saved_data.columns:
			saved_data[column] = saved_data.apply(lambda _: '', axis=1)

	# Append or merge data to the saved dataset
	for index, row in processed_data.iterrows():
		if mode == "merge" and (saved_data.loc[saved_data['id'] == row.id, 'id']).any():
			for column in [column for column in processed_data.columns if column != 'id']:
				saved_data.loc[saved_data['id'] == row.id, column] = row[column]
		else:
			saved_data = saved_data.append(pd.DataFrame([row]))

	saved_data.to_csv(file, index=False)


def get_xpath_regex_content(html_lxml, id_xpath, rule, item_nb=None):
	"""
	For a given rule, get the id and all the data
	:param html_lxml: html content parsed into lxml
	:param id_xpath: id xpath
	:param rule: scrapping rule
	:param item_nb: number of items if it is a list of item
	:return: A list of the scrapped data: [id, data_1, data_2...]
	"""
	try:
		id_value = get_xpath_content(html_lxml, id_xpath)
		id_value = get_regex_content(id_value, rule['id']['regex'])
		id_value = clean_text(id_value)
		if 'formatter' in rule['id'] and rule['id']['formatter']:
			id_value = rule['id']['formatter'](id_value)
		row = [id_value]
		for attribute_nb in range(0, len(rule['data'])):
			xpath_data = rule['data'][attribute_nb]['xpath']
			if item_nb is not None:
				xpath_data = get_xpath(xpath_data, rule['data'][attribute_nb]['xpath_param'], item_nb)
			data_value = get_xpath_node(html_lxml, xpath_data)
			data_value = get_xpath_content_or_attr(data_value, rule['data'][attribute_nb].get('attribute'))
			data_value = get_regex_content(data_value, rule['data'][attribute_nb]['regex'])
			data_value = clean_text(data_value)
			row.append(data_value)
		return row
	except lxml.etree.XPathEvalError as err:
		print('error lxml.etree.XPathEvalError')
		# Xpath not well formatted
		pass
	except IndexError as err:
		print('error IndexError')
		# Xpath not found, usually because there is no more data in page
		pass
	except TypeError as err:
		print('error TypeError')
		# Regex not found, usually because the regex was not found it the tag
		pass
	except Exception as err:
		print('err 001')
		print(type(err))
		print(err)


def get_xpath(xpath, xpath_param, index):
	return xpath.format(eval(xpath_param.format(index)))


def get_xpath_node(html_lxml, xpath):
	try:
		return html_lxml.xpath(xpath)[0]
	except lxml.etree.XPathEvalError as err:
		print('    Error while searching for xpath {}. It does not seem to be well formatted.'.format(xpath))
		raise err


def get_xpath_content_or_attr(node, attr):
	return node.get(attr) if attr else node.text_content().strip()


def get_xpath_content(html_lxml, xpath):
	try:
		return get_xpath_node(html_lxml, xpath).text_content().strip()
	except IndexError as err:
		xpath_partial = ''
		for xpath_node in xpath.split('/'):
			if xpath_node == '':
				continue
			xpath_partial = xpath_partial + '/' + xpath_node
			try:
				get_xpath_node(html_lxml, xpath_partial).text_content().strip()
			except IndexError:
				print('    Error while searching for xpath {}. {} was not found. Might be normal in is_list_of_items mode.'.format(xpath, xpath_partial))
				break
		raise err


def get_regex_content(text, regex):
	try:
		return search(regex, text, flags=MULTILINE | DOTALL)[0]
	except TypeError as err:
		print('    Error while searching for regex "{}" in (reformatted) string: {}.'.format(regex, clean(text)))
		raise err


def clean_text(text):
	res = text
	res = res.replace(' ', ' ')
	res = compile(r'\s+').sub(' ', res)
	return res
