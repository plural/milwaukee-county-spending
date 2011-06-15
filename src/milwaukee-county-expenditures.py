#!/usr/bin/env python

from BeautifulSoup import BeautifulStoneSoup
from mechanize import Browser
import re
import string
import sys

def formatMoney(value):
  return re.sub('[\$,]', '', value)

url_base = 'http://mcap.milwaukeecounty.org/MAP/Expenditures/Categories/'
by_category_url = 'Default.aspx'

mech = Browser()

print '"%s","%s","%s","%s","%s","%s","%s","%s"' % (
  "Category", "Category Total", "Category Detail",
  "category Detail URL", "Category Detail Total",
  "Vendor Name", "Vendor URL", "Amount")
by_category_page = mech.open(url_base + by_category_url)
by_category_html = by_category_page.read()

soup = BeautifulStoneSoup(by_category_html,
                          convertEntities=BeautifulStoneSoup.ALL_ENTITIES)

category_table = soup.find('table', id='grdCategories')
# skip the header row
for row in category_table.findAll('tr')[1:]:
  category_link_tag = row.findAll('a')[0]
  category_detail_page_url = url_base + category_link_tag['href']
  category_description = category_link_tag.string.strip()
  category_total = formatMoney(row.findAll('td')[1].string)
  #print '%s\t%s\t%s' % (category_description,
  #                      category_detail_page_url,
  #                      category_total)
  #print "\n"

  # Go to category detail page
  sys.stderr.write("Following [%s] to (%s)\n" % (category_description,
                                                 category_detail_page_url))
  mech_category_detail_link_tag = mech.find_link(url=category_link_tag['href'])
  category_detail_page = mech.follow_link(mech_category_detail_link_tag)
  #print "New Page Title: %s" % mech.title()

  # parse category detail description page
  category_detail_soup = BeautifulStoneSoup(category_detail_page.read(),
      convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
  category_detail_table = category_detail_soup.find('table', id='grdCategories')
  for row in category_detail_table.findAll('tr')[1:]:
    category_detail_link_tag = row.findAll('a')[0]
    category_detail_vendor_page_url = (url_base +
                                      category_detail_link_tag['href'])
    category_detail_description = category_detail_link_tag.string.strip()
    category_detail_total = formatMoney(row.findAll('td')[1].string)
    #print '[Details]: %s\t%s\t%s' % (category_detail_description,
    #                      category_detail_vendor_page_url,
    #                      category_detail_total)
    # Go to vendors page for this category
    sys.stderr.write("[Details]: Following [%s] to (%s)\n" %
        (category_detail_description, category_detail_vendor_page_url))
    mech_category_detail_vendor_link_tag = mech.find_link(
        url=category_detail_link_tag['href'])
    vendor_page = mech.follow_link(mech_category_detail_vendor_link_tag)
    #print "[Details]: New Page Title: %s" % mech.title()
    vendor_soup = BeautifulStoneSoup(vendor_page.read(),
        convertEntities=BeautifulStoneSoup.ALL_ENTITIES)
    vendor_table = vendor_soup.find('table', id='grdVendors')
    for row in vendor_table.findAll('tr')[1:]:
      vendor_name = row.findAll('td')[0].string.strip()
      vendor_amount = formatMoney(row.findAll('td')[1].string)
      #print "[Vendor]: %s\t%s" % (vendor_name, vendor_amount)
      print '"%s",%s,"%s","%s",%s,"%s","%s",%s' % (
        category_description, category_total,
        category_detail_description, category_detail_page_url,
        category_detail_total,
        vendor_name, category_detail_vendor_page_url,
        vendor_amount)
    mech.back()
  mech.back()
