#!/bin/python

import sys
import urllib2
import xml.etree.cElementTree as ElementTree
import simplejson as json
import datetime
import StringIO
import gzip
import time
import calendar
import math
import pprint

# Parse the diff and write out a simplified version
class OscHandler():
  def __init__(self):
    self.changes = {}
    self.nodes = {}
    self.ways = {}
    self.relations = {}
    self.action = ""
    self.primitive = {}
    self.missingNds = set()

  def startElement(self, name, attributes):
    if name in ('modify', 'delete', 'create'):
      self.action = name
    if name in ('node', 'way', 'relation'):
      self.primitive['id'] = int(attributes['id'])
      self.primitive['version'] = int(attributes['version'])
      self.primitive['changeset'] = int(attributes['changeset'])
      self.primitive['user'] = attributes.get('user')
      self.primitive['timestamp'] = isoToTimestamp(attributes['timestamp'])
      self.primitive['tags'] = {}
      self.primitive['action'] = self.action
    if name == 'node':
      self.primitive['lat'] = float(attributes['lat'])
      self.primitive['lon'] = float(attributes['lon'])
    elif name == 'tag':
      key = attributes['k']
      val = attributes['v']
      self.primitive['tags'][key] = val
    elif name == 'way':
      self.primitive['nodes'] = []
    elif name == 'relation':
      self.primitive['members'] = []
    elif name == 'nd':
      ref = int(attributes['ref'])
      self.primitive['nodes'].append(ref)
      if ref not in self.nodes:
        self.missingNds.add(ref)
    elif name == 'member':
      self.primitive['members'].append(
                                    {
                                     'type': attributes['type'],
                                     'role': attributes['role'],
                                     'ref': attributes['ref']
                                    })

  def endElement(self, name):
    if name == 'node':
      self.nodes[self.primitive['id']] = self.primitive
    elif name == 'way':
      self.ways[self.primitive['id']] = self.primitive
    elif name == 'relation':
      self.relations[self.primitive['id']] = self.primitive
    if name in ('node', 'way', 'relation'):
      self.primitive = {}

def isoToTimestamp(isotime):
  t = datetime.datetime.strptime(isotime, "%Y-%m-%dT%H:%M:%SZ")
  return calendar.timegm(t.utctimetuple())

def distanceBetweenNodes(node1, node2):
  dlat = math.fabs(node1['lat'] - node2['lat'])
  dlon = math.fabs(node1['lon'] - node2['lon'])
  return math.hypot(dlat, dlon)

def parseOsm(source, handler):
  for event, elem in ElementTree.iterparse(source, events=('start', 'end')):
    if event == 'start':
      handler.startElement(elem.tag, elem.attrib)
    elif event == 'end':
      handler.endElement(elem.tag)
    elem.clear() 

def collateData(collation, firstAxis, secondAxis):
  if firstAxis not in collation:
    collation[firstAxis] = {}
  
  first = collation[firstAxis]

  if secondAxis not in first:
    first[secondAxis] = 0
  
  first[secondAxis] = first[secondAxis] + 1

  collation[firstAxis] = first

user_collation = {}
changeset_collation = {}
time_collation = {}
time_user_collation = {}

def minutelyUpdateRun(state):
  minuteNumber = int(isoToTimestamp(state['timestamp'])) / 60

  # Grab the next sequence number and build a URL out of it
  sqnStr = state['sequenceNumber'].zfill(9)
  url = "http://planet.openstreetmap.org/minute-replicate/%s/%s/%s.osc.gz" % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])

  print "Downloading change file (%s)." % (url)
  content = urllib2.urlopen(url)
  content = StringIO.StringIO(content.read())
  gzipper = gzip.GzipFile(fileobj=content)

  print "Parsing change file."
  handler = OscHandler()
  parseOsm(gzipper, handler)

  print "%d nodes, %d ways, %d relations." % (len(handler.nodes), len(handler.ways), len(handler.relations))

  for node in handler.nodes.values():
    #collateData(user_collation, node['user'], node['action'])
    #collateData(changeset_collation, node['changeset'], node['action'])
    collateData(time_collation, int(node['timestamp']) * 1000, node['action'])
    collateData(time_user_collation, int(node['timestamp']) * 1000, node['user'])
  for way in handler.ways.values():
    #collateData(user_collation, way['user'], way['action'])
    #collateData(changeset_collation, way['changeset'], way['action'])
    collateData(time_collation, int(way['timestamp']) * 1000, way['action'])
    collateData(time_user_collation, int(way['timestamp']) * 1000, way['user'])
  for relation in handler.relations.values():
    #collateData(user_collation, relation['user'], relation['action'])
    #collateData(changeset_collation, relation['changeset'], relation['action'])
    collateData(time_collation, int(relation['timestamp']) * 1000, relation['action'])
    collateData(time_user_collation, int(relation['timestamp']) * 1000, relation['user'])

#  time_list = []
#  for (timet, entry) in time_collation.iteritems():
#    time_list.append((timet,
#        entry['create'] if 'create' in entry else None,
#        entry['modify'] if 'modify' in entry else None,
#        entry['delete'] if 'delete' in entry else None))
 
  f = open('current.json', 'w')
  f.write(json.dumps(time_collation, sort_keys=True, indent=True))
  f.close()
  time_collation.clear()

def readState():
  # Read the state.txt
  sf = open('state.txt', 'r')

  state = {}
  for line in sf:
    if line[0] == '#':
      continue
    (k, v) = line.split('=')
    state[k] = v.strip().replace("\\:", ":")
  
  sf.close()

  return state

def fetchNextState(currentState):
  # Download the next state file
  nextSqn = int(currentState['sequenceNumber']) + 1
  sqnStr = str(nextSqn).zfill(9)
  url = "http://planet.openstreetmap.org/minute-replicate/%s/%s/%s.state.txt" % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])
  try:
    u = urllib2.urlopen(url)
    statefile = open('state.txt', 'w')
    statefile.write(u.read())
    statefile.close()
  except Exception, e:
    print e
    return False

  return True 

if __name__ == "__main__":
  while True:
    state = readState()
    
    start = time.time()
    minutelyUpdateRun(state)
    elapsed = time.time() - start

    stateTs = datetime.datetime.strptime(state['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
    nextTs = stateTs + datetime.timedelta(minutes=1)

    if datetime.datetime.utcnow() < nextTs:
      timeToSleep = (nextTs - datetime.datetime.utcnow()).seconds + 13.0
    else:
      timeToSleep = 0.0
    print "Waiting %2.1f seconds for the next state.txt." % (timeToSleep)
    time.sleep(timeToSleep)
    
    result = fetchNextState(state)

    if not result:
      print "Couldn't continue. Sleeping %2.1f more seconds." % (15.0)
      time.sleep(15.0)
