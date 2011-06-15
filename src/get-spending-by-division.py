#!/usr/bin/env python

from BeautifulSoup import BeautifulStoneSoup
from Queue import Queue
from threading import Lock
from threading import Thread
import mechanize
import pprint
import re
import string
import sys

""" Strip the dollar sign and comma to make importing easier later."""
def formatMoney(value):
  return re.sub('[\$,]', '', value)

""" Worker method used by a thread to process a department page."""
def departmentPageWorker(thread_num, l, destination):
  while (True and processDepartments):
    request = q.get()
    response = mechanize.urlopen(request).read()
    depts = parseDepartmentTable(response)
    l.acquire()
    sys.stderr.write("thread %d processing department page\n" % thread_num)
    destination.extend(depts)
    l.release()
    q.task_done()


""" Parses all of the department records from a department page. """
def parseDepartmentTable(department_page):
  departments = []
  department_soup = BeautifulStoneSoup(department_page,
      convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
  division_table = department_soup.find('table', id='grdAgency')
  for row in division_table.findAll('tr')[1:]:
    department_fiscal_year = row.findAll('td')[0].string
    department_name = row.findAll('td')[1].string.strip()
    division_link = row.findAll('a')[0]
    division_detail_page_url = url_base + division_link['href']
    division_name = division_link.string.strip()
    division_total = formatMoney(row.findAll('td')[3].string)
    departments.append({
        'fiscal_year': department_fiscal_year,
        'department_name': department_name,
        'division_name': division_name,
        'division_detail_page_url': division_detail_page_url,
        'division_total': division_total,
        'categories': [],
      })
  return departments

""" Worker method used by a thread to process a category page."""
def categoryWorker(thread_num, l):
  while (True and processCategories):
    department = category_queue.get()
    sys.stderr.write('Parsing categories for FY %s department %s, division %s at %s\n' % (
      department['fiscal_year'], department['department_name'],
      department['division_name'], department['division_detail_page_url']))
    my_mech = mechanize.Browser()
    category_request = my_mech.open(department['division_detail_page_url'])
    category_response = category_request.read()
    category_soup = BeautifulStoneSoup(category_response,
       convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
    category_table = category_soup.find('table', id='grdAgency')
    category_page_dropdown = category_soup.find('select',
        id='MozillaPager1_ddlPageNumber')
    category_pages = []
    if category_page_dropdown:
      for category_page in category_page_dropdown.findAll('option'):
        category_pages.append(category_page['value'])
    else:
      sys.stderr.write("No page drop down on %s.\n" % department['division_detail_page_url'])

    department['categories'].extend(parseCategoryTable(category_response))

    for page in category_pages[1:]:
      sys.stderr.write(' ... Page %s from %s\n' % (page, department['division_detail_page_url']))
      my_mech.select_form('ctl02')
      my_mech.form.set_value([page], 'MozillaPager1$ddlPageNumber')
      category_page_request = my_mech.form.click('MozillaPager1$btnPageNumber')
      local_categories = parseCategoryTable(mechanize.urlopen(category_page_request).read())
      department['categories'].extend(local_categories)
    category_queue.task_done()


""" Parses all of the rows out of a category detail page.

Returns a list of detail row tuples."""
def parseCategoryTable(category_page):
  categories = []
  category_page_soup = BeautifulStoneSoup(category_page,
      convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
  category_page_table = category_page_soup.find('table', id='grdAgency')
  # skip the header row
  if category_page_table:
    for row in category_page_table.findAll('tr')[1:]:
      category_detail_link_tag = row.findAll('a')[0]
      # apparently some of the urls are a bit wonky and have spaces in them.
      category_detail_page_url = (url_base + category_detail_link_tag['href']).replace(" ", "")
      category_description = category_detail_link_tag.string.strip()
      category_total = formatMoney(row.findAll('td')[1].string)
      categories.append({
        'description': category_description,
        'total': category_total,
        'detail_url': category_detail_page_url,
        'details': [],
      })
  return categories

""" Worker method used by a thread to process a category page."""
def categoryDetailWorker(thread_num, l):
  while (True and processCategoryDetails):
    category = category_details_queue.get()
    sys.stderr.write('Parsing category details for %s at %s\n' % (
      category['description'], category['detail_url']))
    my_mech = mechanize.Browser()
    category_detail_request = my_mech.open(category['detail_url'])
    category_detail_response = category_detail_request.read()
    category_detail_soup = BeautifulStoneSoup(category_detail_response,
       convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
    category_detail_table = category_detail_soup.find('table', id='grdAgency')
    category_detail_page_dropdown = category_detail_soup.find('select',
        id='MozillaPager1_ddlPageNumber')
    category_detail_pages = []
    if category_detail_page_dropdown:
      for category_detail_page in category_detail_page_dropdown.findAll('option'):
        category_detail_pages.append(category_detail_page['value'])
    else:
      sys.stderr.write("No page drop down on %s.\n" % department['division_detail_page_url'])

    category_details = parseCategoryDetailTable(category_detail_response)
    l.acquire()
    category['details'].extend(category_details)
    l.release()

    for page in category_detail_pages[1:]:
      sys.stderr.write(' ... Page %s from %s\n' % (page, department['division_detail_page_url']))
      my_mech.select_form('ctl02')
      my_mech.form.set_value([page], 'MozillaPager1$ddlPageNumber')
      category_detail_page_request = my_mech.form.click('MozillaPager1$btnPageNumber')
      category_details = parseCategoryDetailTable(mechanize.urlopen(category_detail_page_request).read())
      l.acquire()
      category['details'].extend(category_details)
      l.release()
    category_details_queue.task_done()

""" Parses all of the rows on a category detail list page.

Returns a list of category detail rows."""
def parseCategoryDetailTable(detail_page):
  details = []
  detail_soup = BeautifulStoneSoup(detail_page,
     convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
  detail_table = detail_soup.find('table', id='grdAgency')
  for row in detail_table.findAll('tr')[1:]:
    vendor_url = (url_base + row.findAll('a')[0]['href'].replace(" ", ""))
    details.append({
      'detail_description': row.findAll('a')[0].string.strip(),
      'vendor_url': vendor_url,
      'total': formatMoney(row.findAll('td')[1].string),
      'vendors': [],
    })
  return details

""" Worker method used by a thread to process a category detail vendor."""
def vendorWorker(thread_num, l):
  while (True and processVendors):
    detail = vendors_queue.get()
    sys.stderr.write('Parsing vendors for category detail for %s at %s\n' % (
      detail['detail_description'], detail['vendor_url']))
    my_mech = mechanize.Browser()
    vendor_request = my_mech.open(detail['vendor_url'])
    vendor_response = vendor_request.read()
    vendor_soup = BeautifulStoneSoup(vendor_response,
       convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
    vendor_table = vendor_soup.find('table', id='grdAgency')
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
  vendor_table = vendor_soup.find('table', id='grdAgency')
  for row in vendor_table.findAll('tr')[1:]:
    vendors.append({
      'name': row.findAll('td')[0].string.strip(),
      'amount': formatMoney(row.findAll('td')[1].string)
    })
  return vendors

url_base = 'http://mcap.milwaukeecounty.org/MAP/Expenditures/Agencies/'
by_department_url = 'Default.aspx?year=0'
mech = mechanize.Browser()
by_department_page = mech.open(url_base + by_department_url)
by_department_html = by_department_page.read()
soup = BeautifulStoneSoup(by_department_html,
                          convertEntities=BeautifulStoneSoup.ALL_ENTITIES)

department_page_dropdown = soup.find('select', id='MozillaPager1_ddlPageNumber')
pages = []
for page in department_page_dropdown.findAll('option'):
  pages.append(page['value'])

departments = []

processDepartments = True
q = Queue()
workers = []
lock = Lock()
for i in range(25):
  t = Thread(target=departmentPageWorker, args=(i, lock, departments))
  t.setName("Department-num-%d" % i)
  t.setDaemon(True)
  workers.append(t)
  t.start()

for page in pages:
  sys.stderr.write('Adding department page %s to thread pool\n' % page)
  mech.select_form('ctl02')
  mech.form.set_value([page], 'MozillaPager1$ddlPageNumber')
  q.put(mech.form.click('MozillaPager1$btnPageNumber'))

q.join()
processDepartments = False
# pprint.pprint(departments, sys.stderr)

category_queue = Queue()
category_workers = []
processCategories = True
for i in range(25):
  t = Thread(target=categoryWorker, args=(i, lock))
  t.setName("Category-num-%d" % i)
  t.setDaemon(True)
  category_workers.append(t)
  t.start()

## loaded all top level departments, so let's move on to the detail pages.
for department in departments:
  category_queue.put(department)

category_queue.join()
processCategories = False
# pprint.pprint(departments, sys.stderr)

category_details_queue = Queue()
category_detail_workers = []
processCategoryDetails = True
for i in range(40):
  t = Thread(target=categoryDetailWorker, args=(i, lock))
  t.setName("Category-detail-num-%d" % i)
  t.setDaemon(True)
  category_detail_workers.append(t)
  t.start()

## loaded all top level departments, so let's move on to the detail pages.
for department in departments:
  for category in department['categories']:
    category_details_queue.put(category)

category_details_queue.join()
processCategoryDetails = False
# pprint.pprint(departments[0:10], sys.stderr)

vendors_queue = Queue()
vendor_workers = []
processVendors = True
for i in range(40):
  t = Thread(target=vendorWorker, args=(i, lock))
  t.setName("Vendor-num-%d" % i)
  t.setDaemon(True)
  vendor_workers.append(t)
  t.start()

## process all of the vendor pages.
for department in departments:
  for category in department['categories']:
    for detail in category['details']:
      vendors_queue.put(detail)

vendors_queue.join()
processVendors = False
pprint.pprint(departments, sys.stderr)


### dump out the results
no_vendors_string = '"",0'
no_details_string = '"",0,"",%s' % no_vendors_string
no_categories_string = '"",0,"",%s' % no_details_string
print '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"' % (
  "Fiscal Year", "Department",
  "Division", "Division Total", "Division Detail URL",
  "Category", "Category Total", "Category Detail URL",
  "Category Detail", "Category Detail Total", "Vendor URL",
  "Vendor Name", "Vendor Amount")
for department in departments:
  department_string = '"%s","%s","%s","%s","%s"' % (department['fiscal_year'],
      department['department_name'], department['division_name'],
      department['division_total'], department['division_detail_page_url'])

  if len(department['categories']) == 0:
    print '%s,%s' % (department_string, no_categories_string)
    continue
  for category in department['categories']:
    category_string = '"%s",%s,"%s"' % (category['description'],
        category['total'], category['detail_url'])
    if len(category['details']) == 0:
      print '%s,%s,%s' % (department_string, category_string, no_details_string)
      continue
    for detail in category['details']:
      details_string = '"%s",%s,"%s"' % (detail['detail_description'],
          detail['total'], detail['vendor_url'])
      if len(detail['vendors']) == 0:
        print '%s,%s,%s,%s' % (department_string, category_string,
            details_string, no_vendors_string)
        continue
      for vendor in detail['vendors']:
        vendor_string = '"%s",%s' % (vendor['name'], vendor['amount'])
        print '%s,%s,%s,%s' % (department_string, category_string,
            details_string, vendor_string)
