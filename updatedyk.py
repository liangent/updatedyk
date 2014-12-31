#!/usr/bin/env python
# -*- coding: utf8 -*-

# NOTE: use assert if sending requests manually?

from wmfwikis import getSite
from mw import *
from datetime import datetime, timedelta
from time import time
import re
import traceback
from os import environ

DELTA = timedelta(hours=8)
DELTA_PREC = timedelta(minutes=10)
STAGES = [
	timedelta(days=-1),
	timedelta(days=4),
	timedelta(days=7),
]

now = datetime.utcnow()

sign_re = re.compile(ur'(?P<year>\d{4})年(?P<month>\d{1,2})月(?P<day>\d{1,2})日 \(.\) (?P<hour>\d{1,2}):(?P<minute>\d{1,2}) \(UTC\)')
updateddyk_re = re.compile(ur'\{\{\s*UpdatedDYK\s*\|\s*(.+?)\s*\|\s*(\d+)\s*\}\}', re.IGNORECASE)
updateddyknom_re = re.compile(ur'\{\{\s*UpdatedDYKNom\s*\|\s*(.+?)\s*\}\}', re.IGNORECASE)
dykinvite_re = re.compile(ur'\{\{\s*DYK ?Invite\s*\}\}', re.IGNORECASE)
produce_re = re.compile(ur'\{\{\s*produceEncouragement\s*\|\s*\d+\s*\}\}', re.IGNORECASE)

def clean_tail_newsection(text):
	origtext = text
	while True:
		text = re.sub(u'\n=.+=$', u'', text).rstrip()
		text = re.sub(u'\n\{\{\s*DYKCsplit\s*\}\}$', u'', text).rstrip()
		if text == origtext:
			return text
		origtext = text

class NormalizedTemplate(object):
	def __init__(self, name, content, required):
		self.name = name.strip()
		self.params = {}
		self.ordering = []
		for line in content.split(u'\n'):
			line = line.strip()
			if line.startswith(u'|') and u'=' in line:
				pos = line.find(u'=')
				pname = line[1:pos].strip()
				pvalue = line[pos + 1:].strip()
				self.ordering.append(pname)
				self.params[pname] = pvalue
		for req in required:
			if req not in self.params:
				self.params[req] = u''
	
	def __unicode__(self):
		r = u'{{ %s\n' % self.name
		params = self.params.copy()
		for pn in self.ordering:
			if pn in params:
				r += u' | %s = %s\n' % (pn, params[pn])
				del params[pn]
		for i in params.items():
			r += u' | %s = %s\n' % i
		r += u'}}'
		return r

class DYKEntry(object):
	def __init__(self, content, site, clean=True, quick=False):
		self.removed = False
		self.new_date_section = False
		if u'\n}}' in content:
			endpos = content.find(u'\n}}') + 3
			self.broken = False
			self.tail = clean_tail_newsection(content[endpos:].rstrip())
			self.template = NormalizedTemplate(
				u'DYKEntry', content[:endpos],
				[u'article', u'question', u'image', u'type', u'author', u'nominator', u'hash', u'result'],
			)
			if not clean:
				return
			if not quick:
				article = self.template.params[u'article']
				article_page = site.getPageForDisplay(article)
				if article_page:
					self.template.params[u'article'] = article_page.title
					if article_page.exists:
						change_template(site, u'Talk:' + article_page.title, dykinvite_re, u'{{DYK Invite}}', append=False)
				image = self.template.params[u'image']
				if image:
					image_page = site.getPageForDisplay(u'File:%s' % image)
					if image_page and (
						(site(u'Category:合理使用图像', True) in image_page.referencing(member=True))
					#or
					#	(article_page and (image_page not in article_page.referencing(use=True)))
					):
						self.template.params[u'image'] = u''
			self.template.params[u'question'] = re.sub(ur'<(s|del|strike)>.*?</\1>', u'', self.template.params[u'question'])
			self.template.params[u'question'] = re.sub(ur'(\s*[（\(][^（\(）\)]*?[）\)])+$', u'', self.template.params[u'question'])
			self.template.params[u'author'] = re.sub(ur'<!--.*?-->', u'', self.template.params[u'author'])
			self.template.params[u'nominator'] = re.sub(ur'<!--.*?-->', u'', self.template.params[u'nominator'])
			if not self.get_timestamp():
				self.template.params[u'timestamp'] = str(int(time()))
			self.template.params[u'hash'] = unicode(self.hash_str())
		else:
			self.broken = True
			self.tail = clean_tail_newsection(content.rstrip())
	
	def __unicode__(self, withheader=True):
		if self.removed:
			return u''
		elif self.broken:
			return u'\n\n' + self.tail
		else:
			ts = self.get_timestamp()
			return u'\n' + (((
				(u'=== %d月%d日 ===\n' % (ts.month, ts.day)) if self.new_date_section else u''
			) + u'==== ====\n') if withheader else u'') + unicode(self.template) + self.tail + (
				u'\n\n==== ====\n{{DYKCsplit}}' if withheader else u''
			)

	def get_ts_stage(self, nowref=now):
		ts = self.get_timestamp()
		if not ts:
			return 0
		diff = max(nowref - ts, timedelta())
		i = 0
		while i < len(STAGES) and diff >= STAGES[i]:
			i += 1
		return i - 1, diff
	
	def hash_str(self): # do not use __hash__
		import hashlib
		sha1 = hashlib.sha1()
		for k in [u'article', u'question', u'image', u'type', u'author', u'nominator', u'timestamp']:
			sha1.update(hashlib.sha1(self.template.params[k].encode('utf8')).digest())
		return sha1.hexdigest()
	
	def get_timestamp(self):
		try:
			return datetime.utcfromtimestamp(int(self.template.params['timestamp']))
		except Exception:
			pass
	
	# True = passed
	# False = rejected
	# None = skip
	def check_result(self, site, page, debug):
		if self.broken:
			if debug:
				print 'check broken'
			return None
		if self.hash_str() == environ.get('UPDATEDYK_FORCE'):
			if debug:
				print 'check force'
			return True
		result = self.template.params[u'result']
		if result.count(u'|') != 2:
			if debug:
				print 'check badresult1'
			return None
		try:
			result, hash, timestamp = result.split(u'|')
		except ValueError:
			if debug:
				print 'check badresult2'
			return None
		result = result.strip()
		hash = hash.strip()
		if hash != self.hash_str():
			if debug:
				print 'check badhash'
			return None
		timestamp = timestamp.strip()
		try:
			timestamp = datetime.utcfromtimestamp(int(timestamp))
		except ValueError:
			if debug:
				print 'check badts'
			return None
		curstage, curdiff = self.get_ts_stage()
		tagstage, tagdiff = self.get_ts_stage(timestamp)
		if debug:
			print 'stage: cur', curdiff, curstage, 'tag', tagdiff, tagstage
		if result not in (u'+', u'-', u'*', u'!'):
			if debug:
				print 'check unkresult'
			return None
		if result == u'+' and curstage <= tagstage:
			if debug:
				print 'check badstage+'
			return None
		if result == u'-' and tagstage + 1 < len(STAGES):
			if debug:
				print 'check badstage-'
			return None
		apires = site._apiRequest(
			action = 'query',
			prop = 'revisions',
			titles = page,
			rvlimit = '1',
			rvprop = 'user|comment',
			rvdir = 'newer',
			rvstart = timestamp.strftime('%Y%m%d%H%M%S'),
		)['query']['pages'].values()[0]
		if 'revisions' not in apires:
			if debug:
				print 'check norev'
			return None
		apires = apires['revisions'][0]
		if u'%s|%s' % (result, hash) not in apires.get('comment', u''):
			if debug:
				print 'check badcomment'
			return None
		# Validate sysop right
		apires = site._apiRequest(
			action = 'query',
			list = 'users',
			ususers = apires['user'],
			usprop = 'groups',
		)['query']['users'][0]
		if u'sysop' in apires.get('groups', []):
			if debug:
				print 'check pass'
			if result in (u'+', u'*'):
				return True
			else:
				return False
		if debug:
			print 'check baduser'
		return None

class DYKCPage(object):
	def __init__(self, content, site, clean=True, quick=False):
		parts = re.split(ur'\n\{\{ ?DYKEntry', content)
		self.header = clean_tail_newsection(parts[0].strip())
		parts = parts[1:]
		for i in xrange(len(parts)):
			parts[i] = u'{{ DYKEntry' + parts[i]
		self.entries = [DYKEntry(x.strip(), site, clean, quick) for x in parts]
		if not clean:
			return
		prevdate = None
		# Add date sections
		for entry in self.entries:
			if entry.broken:
				continue
			ts = entry.get_timestamp()
			if ts and ts.date() != prevdate:
				prevdate = ts.date()
				entry.new_date_section = True
		# Check duplicates
		articles = set()
		self.count = 0
		for entry in reversed(self.entries):
			if entry.broken:
				continue
			self.count += 1
			if entry.template.params[u'article'] not in articles:
				articles.add(entry.template.params[u'article'])
			else:
				entry.template.params[u'result'] = u''
				entry.template.params[u'bot'] = u'duplicate'
	
	def __unicode__(self):
		r = self.header
		for entry in self.entries:
			r += unicode(entry)
		return r

class DYKPage(object):
	def __init__(self, content):
		if not u'\n{{ Dyk/auto' in content:
			raise ValueError('Bad DYK page format: No start tag')
		startpos = content.find(u'\n{{ Dyk/auto') + 1
		endpos = content.find(u'\n}}', startpos) + 3
		if endpos == -1:
			raise ValueError('Bad DYK page format: No end tag')
		self.head = content[:startpos]
		self.tail = content[endpos:]
		self.template = NormalizedTemplate(
			u'Dyk/auto', content[startpos:endpos],
			[
				u'0', u'p0', u'1', u'p1', u'2', u'p2', u'3', u'p3', u'4', u'p4', u'5', u'p5',
				u't0', u't1', u't2', u't3', u't4', u't5', # types, for check
			],
		)
		self.build_entries()
	
	def __unicode__(self):
		self.save_entries()
		return self.head + unicode(self.template) + self.tail
	
	def build_entries(self):
		self.entries = [
			dict(
				question = self.template.params[u'%d' % x],
				image = self.template.params[u'p%d' % x],
				type = self.template.params[u't%d' % x],
			)
			for x in xrange(6)
		]
	
	def save_entries(self):
		for x in xrange(6):
			self.template.params[u'%d' % x] = self.entries[x]['question']
			self.template.params[u'p%d' % x] = self.entries[x]['image']
			self.template.params[u't%d' % x] = self.entries[x]['type']

# returns MatchObject
def change_template(site, pagename, regex, replace, default=None, append=True):
	if default is None:
		default = replace
	page = site(pagename)
	try:
		cur = page.current
	except MediaWikiException:
		page += Revision(default)
		return None
	match = regex.search(cur.content)
	if match:
		new_cont = regex.sub(replace, cur.content)
		if new_cont != cur.content:
			page += Revision(new_cont)
	else:
		if append:
			whattext = 'appendtext'
			usetext = u'\n' + default
		else:
			whattext = 'prependtext'
			usetext = default + u'\n'
		req = dict(
			action = 'edit',
			title = pagename,
			section = '0',
			token = lambda: site._token('edit'),
		)
		req[whattext] = usetext
		try:
			site._apiRequest(**req)
		except MediaWikiApiError, e:
			if e.code == 'nosuchsection':
				del req[whattext]
				del req['section']
				req['prependtext'] = default + u'\n'
				site._apiRequest(**req)
			else:
				raise
	return match

def maintenance(bot=None):
	if not bot:
		bot = getSite('zh', 'wikipedia', 'bot', apiErrorAutoRetries=10, httpErrorAutoRetries=50)
	dykc = bot(u'Wikipedia:新条目推荐/候选')
	dykclist = bot(u'Wikipedia:新条目推荐/候选/列表')
	# Grab DYKC content
	dykc_cur = dykc.current
	dykc_cont = dykc_cur.content
	dykc_page = DYKCPage(dykc_cont, bot, clean=True, quick=False)
	dykc_newcont = unicode(dykc_page)
	try:
		dykc += Revision(dykc_newcont, base=dykc_cur, bot=True)
	except PageNotSaved:
		maintenance(bot)
	else:
		dykclist += Revision(u'－'.join(
			[u'[[Wikipedia:新条目推荐/候选#%(a)s|%(a)s]]' % {'a': x.template.params[u'article']}
				for x in dykc_page.entries if not x.broken and not x.removed]
		))

def hashremoval(dykc, entryhash, debug, error_log, user):
	if debug:
		print 'hashremoval', entryhash
	dykc_cur = dykc.current
	dykc_cont = dykc_cur.content
	dykc_page = DYKCPage(dykc_cont, user, clean=True, quick=True)
	for entry in dykc_page.entries:
		if entry.broken:
			continue
		if entry.hash_str() in entryhash:
			entry.removed = True
	dykc_newcont = unicode(dykc_page)
	try:
		dykc += Revision(dykc_newcont, base=dykc_cur, bot=True, comment='hashremoval: ' + ', '.join(entryhash))
	except PageNotSaved:
		hashremoval(dykc, entryhash, debug, error_log, user)

def main(debug=False, error_log=None):
	bot = getSite('zh', 'wikipedia', 'bot', apiErrorAutoRetries=10, httpErrorAutoRetries=50)
	sysop = getSite('zh', 'wikipedia', 'sysop', apiErrorAutoRetries=10, httpErrorAutoRetries=50)
	dyk = sysop(u'Template:Dyk')
	dykc = bot(u'Wikipedia:新条目推荐/候选')
	recent = bot(u'Wikipedia:新条目推荐/上一次更新')
	archive = bot(u'Wikipedia:新条目推荐/%d年%d月' % (now.year, now.month)) # I removed cascading protect just now.
	mainpage = bot(u'Wikipedia:首页')
	do_update = True
	# Grab DYKC content
	dykc_cur = dykc.current
	dykc_cont = dykc_cur.content
	dykc_page = DYKCPage(dykc_cont, bot, clean=True, quick=True)
	if dykc_page.count > 6:
		DELTA = timedelta(hours=4)
	if dykc_page.count > 12:
		DELTA = timedelta(hours=3)
	if dykc_page.count > 16:
		DELTA = timedelta(hours=2)
	# Confirm recent update time
	recent_cont = recent.current.content
	recent_datetime = sign_re.match(recent_cont)
	if environ.get('UPDATEDYK_FORCE'):
		recent_datetime = 'ENV_FORCE'
	elif recent_datetime:
		try:
			recent_datetime = datetime(
				int(recent_datetime.group('year')), int(recent_datetime.group('month')), int(recent_datetime.group('day')),
				int(recent_datetime.group('hour')), int(recent_datetime.group('minute')),
			)
		except ValueError:
			recent_datetime = 'ValueError'
		else:
			if now - recent_datetime + DELTA_PREC < DELTA:
				do_update = False
	if debug:
		print 'prev, do_update:', recent_datetime, do_update
	hashremove = []
	if do_update:
		# Recent update
		recent += Revision(u'~~~~~')
		# Grab DYK content
		dyk_cur = dyk.current
		dyk_cont = dyk_cur.content
		dyk_page = DYKPage(dyk_cont)
		# Check entries one by one
		no_type = False
		no_img = False
		for entry in reversed(dykc_page.entries):
			# check_result checks passed, rejected, and .broken as well
			result = entry.check_result(bot, u'Wikipedia:新条目推荐/候选', debug)
			if debug:
				print result, 'BROKEN' if entry.broken else unicode(entry.template)
			if result is None:
				continue
			elif result:
				# Check image, type etc.
				if entry.template.params['type'] and (
					entry.template.params['type'] in [x['type'] for x in dyk_page.entries[:5]]
				):
					no_type = True
					continue
				if not entry.template.params['image'] and not any([x['image'] for x in dyk_page.entries[:5]]):
					no_img = True
					continue
				if debug:
					print 'img&type passed'
				# Template:Dyk
				dyk_page.entries.insert(0, dict(
					question = entry.template.params['question'],
					image = entry.template.params['image'],
					type = entry.template.params['type'],
				))
				if debug:
					print 'archiving'
				# Archive
				if not archive.exists:
					archive += Revision(u'{{DYKMonthlyArchive}}')
				bot._apiRequest(
					action = 'edit',
					title = u'Wikipedia:新条目推荐/存档/%d年%d月' % (now.year, now.month),
					token = lambda: bot._token('edit'),
					prependtext = u'* %s\n' % entry.template.params['question'],
				) # Ditto. It's not protected anymore.
				bot._apiRequest(
					action = 'edit',
					title = u'Wikipedia:新条目推荐/供稿/%d年%d月%d日' % (now.year, now.month, now.day),
					token = lambda: bot._token('edit'),
					prependtext = u'* %s\n' % entry.template.params['question'],
				)
				bot._apiRequest(
					action = 'edit',
					title = u'Wikipedia:新条目推荐/分类存档/未分类',
					token = lambda: bot._token('edit'),
					appendtext = u' [[%s]]' % entry.template.params['article'],
				)
				if debug:
					print 'author&nom-notify'
				# Author
				if entry.template.params['author']:
					match = change_template(
						bot, u'User talk:%s' % entry.template.params['author'], updateddyk_re,
						lambda m: u'{{UpdatedDYK|%s|%d}}' % (entry.template.params['article'], int(m.group(2)) + 1),
						u'{{UpdatedDYK|%s|1}}' % entry.template.params['article'],
					)
					author_dykcount = (int(match.group(2)) if match else 0) + 1
					if author_dykcount % 5 == 0:
						change_template(
							bot, u'User:%s' % entry.template.params['author'], produce_re,
							u'{{produceEncouragement|%d}}' % (author_dykcount / 5), None, False,
						)
				# Nominator
				if entry.template.params['nominator'] and entry.template.params['author'] != entry.template.params['nominator']:
					change_template(
						bot, u'User talk:%s' % entry.template.params['nominator'], updateddyknom_re,
						u'{{UpdatedDYKNom|%s}}' % entry.template.params['article'],
					)
				# Main page
				mainpage.purge()
				if debug:
					print 'talks'
				# Article talk
				talkpage = bot(u'Talk:' + entry.template.params['article'])
				try:
					talkpage_cont = talkpage.current.content
				except PageNotExists:
					talkpage_cont = u''
				# Avoid doing this on a blank content?
				talkpage_cont = dykinvite_re.sub(u'', talkpage_cont)
				entry.template.name = u'DYKEntry/archive'
				entry.template.params[u'revid'] = unicode(dykc_cur.id)
				entry.template.params[u'closets'] = '{{subst:#time:U}}'
				talkpage_ncont = u'{{DYKtalk|%d年|%d月%d日}}' % (now.year, now.month, now.day)
				if talkpage_cont:
					talkpage_ncont += u'\n' + talkpage_cont.strip()
				talkpage_ncont += entry.__unicode__(False)
				talkpage += Revision(talkpage_ncont)
				# Remove from DYKC page
				entry.removed = True
				hashremove.append(entry.hash_str())
				break
			else:
				#continue
				# TODO. after VPM talk ends. archive rejected
				# Archive
				bot._apiRequest(
					action = 'edit',
					title = u'Wikipedia:新条目推荐/未通过/%d年' % now.year,
					token = lambda: bot._token('edit'),
					prependtext = u'* %s\n' % entry.template.params['question'],
				)
				entry.template.name = u'DYKEntry/archive'
				entry.template.params[u'revid'] = unicode(dykc_cur.id)
				entry.template.params[u'closets'] = '{{subst:#time:U}}'
				entry.template.params[u'rejected'] = u'rejected'
				if debug:
					print 'failtalk'
				# Talk
				artpage = bot(entry.template.params['article'])
				if artpage.exists:
					# Article talk
					talkpage = bot(u'Talk:' + entry.template.params['article'])
					try:
						talkpage_cont = talkpage.current.content
					except PageNotExists:
						talkpage_cont = u''
					# Avoid doing this on a blank content?
					talkpage_cont = dykinvite_re.sub(u'', talkpage_cont)
					if talkpage_cont.strip():
						talkpage_ncont = talkpage_cont.strip() + u'\n\n'
					else:
						talkpage_ncont = u''
					#entry.tail = re.sub(ur'\n=.+=$', u'', entry.tail)
					talkpage_ncont += entry.__unicode__(False).strip()
					talkpage += Revision(talkpage_ncont)
				else:
					bot._apiRequest(
						action = 'edit',
						title = u'Wikipedia talk:新条目推荐/未通过/%d年' % now.year,
						token = lambda: bot._token('edit'),
						appendtext = u'\n\n' + entry.__unicode__(False).strip(),
					)
				entry.removed = True
				hashremove.append(entry.hash_str())
		else:
			# TODO: notify community
			if error_log:
				print >>error_log, 'NO_NEW_ENTRY',
				if no_type:
					print >>error_log, 'NO_TYPE',
				if no_img:
					print >>error_log, 'NO_IMG',
			# Reset recent update
			recent += Revision(u'')
		if debug:
			print 'updating dyk page'
		dyk += Revision(unicode(dyk_page))
	if debug:
		print 'updating dykc page'
	dykc_newcont = unicode(dykc_page)
	try:
		dykc += Revision(dykc_newcont, base=dykc_cur, bot=True)
	except PageNotSaved:
		if debug:
			print '(conflicted)'
		hashremoval(dykc, hashremove, debug, error_log, bot)

if __name__ == '__main__':
	try:
		main(True)
	except Exception:
		traceback.print_exc()
