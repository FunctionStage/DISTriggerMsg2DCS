# -*- coding:utf-8 -*-
import json
import redis
import base64


def handler(event, context):
    logger = context.getLogger()
    
    dcsServer = context.getUserData("dcs_server")
    dcsPort = context.getUserData("dcs_port")
    dcsPwd = context.getUserData("dcs_password")
    if dcsServer is None:
        dcsServer = '100.125.1.108'
    if dcsPort is None :
        dcsPort = '1028'
    if dcsPwd is None:
        dcsPwd = 'Huawei@123'
    print 'DCS server: %s:%s' %(dcsServer, dcsPort)

    # connect the DCS service (redis)
    rdcs = redis.StrictRedis(host=dcsServer, port=dcsPort, db=0, password=dcsPwd)

    streamName = event["StreamName"]
    print 'DIS stream name: %s' %(streamName)
    
    records = event["Message"]["records"]
    for r in records:
        sn = r["sequence_number"]
        orginalData = r["data"]
        data = base64.b64decode(orginalData)

        # insert DIS data to DCS by vpc
        rdcs.set(sn, data)

        # read data from DCS by vpc
        dt = rdcs.get(sn)
        print '*** Read data from DCS: [ sn: %s, data: %s ]' % (sn, dt)
    ret = '*** Received %d message from DIS ***' %(len(records))
    print ret
    return ret

