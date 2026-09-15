#!/usr/bin/env python3
import json,sys
from pathlib import Path
root=Path(__file__).resolve().parents[2];mode=sys.argv[1];sid='SCN-M2-PRESSURE-COMPONENT' if mode=='e2e' else 'SCN-M2-PRESSURE-READER-CONTENTION';p=root/'artifacts/boring-cdc-m2-pressure'/sid/'pressure-component-v1'
m=json.load(open(p/'manifest.json'));s=json.load(open(p/'state/after.json'));assert m['result']['status']=='pass' and m['redaction']['secrets_found']==0;assert s['pressure_order']==['normal','warning','action','critical','hard'] and s['gc_transaction_aligned'] and not s['automatic_full_vacuum'];print(json.dumps({'mode':mode,'status':'pass'},sort_keys=True))
