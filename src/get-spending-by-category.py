#!/usr/bin/env python

from BeautifulSoup import BeautifulStoneSoup
from Queue import Queue
from threading import Lock
from threading import Thread
import gflags
import mechanize
import pprint
import re
import string
import sys

FLAGS=gflags.FLAGS

gflags.DEFINE_integer("year", 0, "Only process results from this year.")

""" Strip the dollar sign and comma to make importing easier later."""
def formatMoney(value):
  return re.sub('[\$,]', '', value)

""" Worker method used by a thread to process a category page."""
def categoryWorker(thread_num, l):
  while (True and processCategories):
    request = category_queue.get() 
    sys.stderr.write('Category thread %d processing category page\n' % thread_num)
    response = mechanize.urlopen(request).read()
    cats = parseCategoryTable(response)
    l.acquire()
    categories.extend(cats)
    l.release()
    category_queue.task_done()

""" Parses all of the rows of a category page."""
def parseCategoryTable(category_page):
  categories = []
  category_page_soup = BeautifulStoneSoup(category_page,
      convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
  category_table = category_page_soup.find('table', id='grdCategories')
  # skip the header row
  for row in category_table.findAll('tr')[1:]:
    fiscal_year = row.findAll('td')[0].string
    category_link_tag = row.findAll('a')[0]
    category_detail_page_url = url_base + category_link_tag['href']
    category_description = category_link_tag.string.strip()
    category_total = formatMoney(row.findAll('td')[2].string)
    if FLAGS.year == 0 or FLAGS.year == int(fiscal_year):
      categories.append({
          'fiscal_year': fiscal_year,
          'name': category_description,
          'detail_url': category_detail_page_url,
          'total': category_total,
          'details': [],
          })
  return categories

""" Worker method used by a thread to process a category page."""
def categoryDetailWorker(thread_num, l):
  while (True and processCategoryDetails):
    category = category_details_queue.get()
    sys.stderr.write('Parsing category details for %s at %s\n' % (
      category['name'], category['detail_url']))
    my_mech = mechanize.Browser()
    category_detail_request = my_mech.open(category['detail_url'])
    category_detail_response = category_detail_request.read()
    category_detail_soup = BeautifulStoneSoup(category_detail_response,
       convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
    category_detail_table = category_detail_soup.find('table', id='grdCategories')
    category_detail_page_dropdown = category_detail_soup.find('select',
        id='MozillaPager1_ddlPageNumber')
    category_detail_pages = []
    if category_detail_page_dropdown:
      for category_detail_page in category_detail_page_dropdown.findAll('option'):
        category_detail_pages.append(category_detail_page['value'])
    else:
      sys.stderr.write("No page drop down on %s.\n" % category['detail_url'])

    category_details = parseCategoryDetailTable(category_detail_response)
    l.acquire()
    category['details'].extend(category_details)
    l.release()

    for page in category_detail_pages[1:]:
      sys.stderr.write(' ... Page %s from %s\n' % (page, category['detail_url']))
      my_mech.select_form('ctl02')
      my_mech.form.set_value([page], 'MozillaPager1$ddlPageNumber')
      category_detail_page_request = my_mech.form.click('MozillaPager1$btnPageNumber')
      category_details = parseCategoryDetailTable(mechanize.urlopen(category_detail_page_request).read())
      l.acquire()
      category['details'].extend(category_details)
      l.release()
    category_details_queue.task_done()

""" Parses all of the rows out of a category detail page.

Returns a list of detail row tuples."""
def parseCategoryDetailTable(detail_page):
  details = []
  detail_page_soup = BeautifulStoneSoup(detail_page,
      convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
  detail_page_table = detail_page_soup.find('table', id='grdCategories')
  # skip the header row
  for row in detail_page_table.findAll('tr')[1:]:
    detail_vendor_link_tag = row.findAll('a')[0]
    detail_vendor_page_url = (url_base + detail_vendor_link_tag['href'])
    detail_description = detail_vendor_link_tag.string.strip()
    detail_total = formatMoney(row.findAll('td')[1].string)
    details.append({
      'description': detail_description,
      'total': detail_total,
      'vendor_url': detail_vendor_page_url,
      'vendors': [],
    })
  return details

""" Worker method used by a thread to process a category detail vendor."""
def vendorWorker(thread_num, l):
  while (True and processVendors):
    detail = vendors_queue.get()
    sys.stderr.write('Parsing vendors for category detail for %s at %s\n' % (
      detail['description'], detail['vendor_url']))
    my_mech = mechanize.Browser()
    vendor_request = my_mech.open(detail['vendor_url'])
    vendor_response = vendor_request.read()
    vendor_soup = BeautifulStoneSoup(vendor_response,
       convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
    vendor_table = vendor_soup.find('table', id='grdVendors')
    vendor_page_dropdown = vendor_soup.find('select',
        id='MozillaPager1_ddlPageNumber')
    vendor_pages = []
    if vendor_page_dropdown:
      for vendor_page in vendor_page_dropdown.findAll('option'):
        vendor_pages.append(vendor_page['value'])
    else:
      sys.stderr.write("No page drop down on %s.\n" % detail['vendor_url'])

    vendors = parseVendorTable(vendor_response)
    l.acquire()
    detail['vendors'].extend(vendors)
    l.release()

    for page in vendor_pages[1:]:
      sys.stderr.write(' ... Page %s from %s\n' % (page, detail['vendor_url']))
      my_mech.select_form('ctl02')
      my_mech.form.set_value([page], 'MozillaPager1$ddlPageNumber')
      vendor_page_request = my_mech.form.click('MozillaPager1$btnPageNumber')
      vendors = parseVendorTable(mechanize.urlopen(vendor_page_request).read())
      l.acquire()
      detail['vendors'].extend(vendors)
      l.release()
    vendors_queue.task_done()

""" Parses all of the rows on a vendor list page.

Returns a list of vendor detail rows."""
def parseVendorTable(vendor_page):
  vendors = []
  vendor_soup = BeautifulStoneSoup(vendor_page,
     convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
  vendor_table = vendor_soup.find('table', id='grdVendors')
  for row in vendor_table.findAll('tr')[1:]:
    vendors.append({
      'name': row.findAll('td')[0].string.strip(),
      'amount': formatMoney(row.findAll('td')[1].string)
    })
  return vendors

try:
  argv = FLAGS(sys.argv)
except gflags.FlagsError, e:
  print '%s\\nUsage: %s ARGS\\n%s' % (e, sys.argv[0], FLAGS)
  sys.exit(1)

url_base = 'http://mcap.milwaukeecounty.org/MAP/Expenditures/Categories/'
by_category_url = 'Default.aspx?year=0'
mech = mechanize.Browser()
by_category_page = mech.open(url_base + by_category_url)
by_category_html = by_category_page.read()
soup = BeautifulStoneSoup(by_category_html,
                          convertEntities=BeautifulStoneSoup.ALL_ENTITIES)

category_page_dropdown = soup.find('select', id='MozillaPager1_ddlPageNumber')
pages = []
for page in category_page_dropdown.findAll('option'):
  pages.append(page['value'])

categories = []

lock = Lock()
category_queue = Queue()
category_workers = []
processCategories = True
for i in range(25):
  t = Thread(target=categoryWorker, args=(i, lock))
  t.setName("Category-num-%d" % i)
  t.setDaemon(True)
  category_workers.append(t)
  t.start()

for page in pages:
  sys.stderr.write('Adding category page %s to thread pool\n' % page)
  mech.select_form('ctl01')
  mech.form.set_value([page], 'MozillaPager1$ddlPageNumber')
  category_queue.put(mech.form.click('MozillaPager1$btnPageNumber'))

category_queue.join()
processCategories = False
pprint.pprint(categories, sys.stderr)

category_details_queue = Queue()
category_detail_workers = []
processCategoryDetails = True
for i in range(25):
  t = Thread(target=categoryDetailWorker, args=(i, lock))
  t.setName("Category-detail-num-%d" % i)
  t.setDaemon(True)
  category_detail_workers.append(t)
  t.start()

for category in categories: 
  category_details_queue.put(category)

category_details_queue.join()
processCategoryDetails = False
pprint.pprint(categories, sys.stderr)

vendors_queue = Queue()
vendor_workers = []
processVendors = True
for i in range(25):
  t = Thread(target=vendorWorker, args=(i, lock))
  t.setName("Vendor-num-%d" % i)
  t.setDaemon(True)
  vendor_workers.append(t)
  t.start()

## process all of the vendor pages.
for category in categories:
  for detail in category['details']:
    vendors_queue.put(detail)

vendors_queue.join()
processVendors = False
pprint.pprint(categories, sys.stderr)


## loaded all top level categories, so let's move on to the detail pages.
#for category in categories:
#  sys.stderr.write('Parsing details for FY %s category %s at %s\n' % (
#    category['fiscal_year'], category['name'], category['detail_url']))
#  detail_response = mech.open(category['detail_url']).read()
#  detail_soup = BeautifulStoneSoup(detail_response,
#      convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
#  detail_table = detail_soup.find('table', id='grdCategories')
#  detail_page_dropdown = detail_soup.find('select',
#      id='MozillaPager1_ddlPageNumber')
#  detail_pages = []
#  if detail_page_dropdown:
#    for detail_page in detail_page_dropdown.findAll('option'):
#      detail_pages.append(detail_page['value'])
#  else:
#    sys.stderr.write("No page drop down on %s.\n" % category['detail_url'])
#    category['details'].extend(
#        parseCategoryDetailTable(detail_response))
#
#  for page in detail_pages:
#    sys.stderr.write(' ... Page %s\n' % page)
#    mech.select_form('ctl02')
#    mech.form.set_value([page], 'MozillaPager1$ddlPageNumber')
#    detail_page_request = mech.form.click('MozillaPager1$btnPageNumber')
#    detail_page_response = mechanize.urlopen(detail_page_request)
#    category['details'].extend(
#        parseCategoryDetailTable(detail_page_response.read()))
#
## let's go grab the vendor data now
#for category in categories:
#  for detail in category['details']:
#    sys.stderr.write("Getting vendor data for %s: %s @ %s\n" % (
#      category['name'], detail['description'], detail['vendor_url']))
#    vendor_response = mech.open(detail['vendor_url']).read()
#    vendor_soup = BeautifulStoneSoup(vendor_response,
#        convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
#    vendor_table = vendor_soup.find('table', id='grdVendors')
#    vendor_page_dropdown = vendor_soup.find('select',
#                                            id='MozillaPager1_ddlPageNumber')
#    vendor_pages = []
#    if vendor_page_dropdown:
#      for vendor_page in vendor_page_dropdown.findAll('option'):
#        vendor_pages.append(vendor_page['value'])
#    else:
#      sys.stderr.write("No page drop down on %s.\n" % detail['vendor_url'])
#      detail['vendors'].extend(parseVendorTable(vendor_response))
#
#    for page in vendor_pages:
#      sys.stderr.write(' ... Page %s\n' % page)
#      mech.select_form('ctl02')
#      mech.form.set_value([page], 'MozillaPager1$ddlPageNumber')
#      vendor_page_request = mech.form.click('MozillaPager1$btnPageNumber')
#      vendor_page_response = mechanize.urlopen(vendor_page_request)
#      detail['vendors'].extend(parseVendorTable(vendor_page_response.read()))

# dump out the details
print '"%s","%s","%s","%s","%s","%s","%s","%s","%s"' % (
  "Fiscal Year",
  "Category", "Category Total",
  "Category Detail", "Category Detail URL", "Category Detail Total",
  "Vendor Name", "Vendor URL", "Vendor Amount")
for category in categories:
  #print (("Fiscal Year: %s\nCategory Description: %s\n" +
  #    "Category Detail URL: %s\nCategory Total: %s") % (
  #        category['fiscal_year'], category['name'], category['detail_url'],
  #        category['total']))
  for details in category['details']:
    #print (("  Detail Description: %s\nDetail Vendor URL: %s\n" +
    #    "  Detail Total: %s") % (details['description'], details['vendor_url'],
    #        details['total']))
    for vendor in details['vendors']:
      #print "    Vendor: %s\n    Amount: %s" % (vendor['name'],
      #    vendor['amount'])
      print '%s,"%s",%s,"%s","%s",%s,"%s","%s",%s' % (
        category['fiscal_year'], category['name'], category['total'],
        details['description'], category['detail_url'], details['total'],
        vendor['name'], details['vendor_url'], vendor['amount'])

