from machy.crawl_html import crawl_web, crawl_web_from_csv


rule = {
    # url of the page. "{}" will be replace with the page indexes generate from page_start to page_end and with the
    # extra_pages array
    'url': 'https://github.com/Ennoriel/machyme/tree/27ad8a7548c398b9c52ec0bb789903d89782adfd/{}',
    'page_start': 1,
    'page_end': 1,
    'extra_pages': ['src/routes/blog'],
    'continue_if_error': False,
    # set to `True` if within a page, you want to scrap a list of items (a list result page for example)
    'is_list_of_items': True,
    # if is_list_of_items is set to `True`, defines the range from which to replace in the xpath
    'item_start': 3,
    'item_end': 10,
    'id': {
        # xpath of the 'id' element
        'xpath': '/html/body/div[4]/div/main/turbo-frame/div/div/div/div[3]/include-fragment/div[2]/div/div[{}]/div[2]/span/a',
        # replace the "{}" argument with this multiple of the index range
        'xpath_param': '{}',
        # regex used to identify the id within the text of the HTML node
        'regex': r'.*',
        # optional formatter
        'formatter': None
    },
    'data': [
        # {
        #     'label': '',
        #     'xpath': '/html/body/div[4]/div/main/turbo-frame/div/div/div/div[3]/include-fragment/div[2]/div/div[{}]/div[4]/time-ago',
        #     'attribute': 'datetime',
        #     'xpath_param': '{}',
        #     'regex': r'.*'
        # }
    ]
}


def test_crawl_web():
    crawl_web([rule], './output/html', './output/res.csv', 'merge', 'cp1252')
    crawl_web_from_csv(
        './output/html',
        './output/res.csv',
        'https://raw.githubusercontent.com/Ennoriel/machyme/master/src/routes/blog/',
        'id'
    )

