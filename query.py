#!/root/csctiming/csctimingenv/bin/python3
import os
import numpy as np
import pandas as pd
from requests_futures.sessions import FuturesSession
import requests
import io
import cgi
import uproot
import time
import ROOT
import threading
import runregistry
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool





def main(run_nums, ds, plot_types, rc, ls):    
    #sess.db = os.environ['ADQM_DB']

    print(plot_types)
    TIMEOUT = 5
    BASE_URL = 'https://cmsweb.cern.ch'

    CA_URL = 'https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt'

    def _get_cern_ca(path):
        """Download the CERN ROOT CA to the specified path."""
        try: os.makedirs(os.path.dirname(path))
        except:...
        r_ca = requests.get(CA_URL)
        
        with open(path, 'wb') as f:
            f.write(b'-----BEGIN CERTIFICATE-----\n')
            f.write(base64.b64encode(r_ca.content))
            f.write(b'\n-----END CERTIFICATE-----\n')

   # _get_cern_ca("/afs/cern.ch/user/n/nhurley/.globus/CERN_Root_CA.crt")


    def _parse_run_full_name(full_name):
        """Return the simplified form of a full DQM run name."""
        if VERBOSE >= 2: print('\ndqm.py _parse_run_full_name(full_name = %s)' % full_name)

        if full_name.split('_')[2].startswith('R000'):  ## Format for OfflineData
            """example: DQM_V0001_R000316293__ZeroBias__Run2018A-PromptReco-v2__DQMIO.root => 316293"""
            name = full_name.split('_')[2][1:]
            return str(int(name))
        elif full_name.split('_')[3].startswith('R000'):  ## Format for OnlineData
            """example: DQM_V0001_SiStrip_R000351871.root => 351871"""
            name = full_name.split('_')[3][1:].replace('.root','')
            return str(int(name))
        else:
            raise error("dqm.py _parse_run_full_name({}), failed to parse run number!".format(full_name))
            return 'NULL'


    def _parse_dqm_page(content):

        dqm_rows = []
        tree = lxml.html.fromstring(content)
        tree.make_links_absolute(BASE_URL)

        for tr in tree.xpath('//tr'):
            td_strs = tr.xpath('td//text()')
            td_urls = tr.xpath('td/a/@href')

            full_name = td_strs[0]
            url = td_urls[0]
            size = int(td_strs[1]) if td_strs[1] != '-' else None
            date = td_strs[2]
            name = _parse_run_full_name(full_name) if size else full_name[:-1]

            dqm_rows.append(DQMRow(name, full_name, url, size, date))

        return dqm_rows


    def _fetch_dqm_rows(url, timeout=TIMEOUT):
            """Return a future of DQMRows of a DQM page at url.
            Access the array of DQMRows at _resolve(self._fetch_dqm_rows(...)).data"""

            # Callback to process dqm responses
            def cb(sess, resp):
                resp.data = _parse_dqm_page(resp.text)
            #background_callback=cb,
            return sess.get(url, timeout=timeout, verify = sess.verify, stream=True)

    full_path = 'https://cmsweb.cern.ch/dqm/offline/data/browse/ROOT/OfflineData/Run2022/ZeroBias/0003558xx/DQM_V0001_R000355892__ZeroBias__Run2022C-PromptReco-v1__DQMIO.root'



    runs = [run.strip() for run in run_nums.split(",")]

    new_runs = []
    for run in runs:
        if ':' in run:
            bounds = run.split(':')
            new_runs += [str(new_run) for new_run in range(int(bounds[0]), int(bounds[1]) + 1)]
        else:
            new_runs.append(run)
    runs = new_runs

    print(runs)


    runs_int = [int(run) for run in runs]
    if rc == "Collisions":
        print(rc)
        request = runregistry.get_runs(filter={
        'class': {'or': ['Collisions22', 'Collisions18']},
        'run_number':{
        'and':[
            {'>=': min(runs_int)},
            {'<=': max(runs_int)},
            ]
        }
        })
    else:
        other_thing = "Express"
        ds = "StreamExpressCosmics"
        request = runregistry.get_runs(filter={
        'class': {'or': ['Cosmics22', 'Cosmics18', 'Commissioning']},
        'run_number':{
        'and':[
            {'>=': min(runs_int)},
            {'<=': max(runs_int)},
            ]
        }
        })


    min_ls_duration = int(ls)
    valid_runs = []
    valid_dates = []
    for run in request:
        if int(run['oms_attributes']['ls_duration']) < min_ls_duration: continue
        valid_runs += [str(run['oms_attributes']['run_number'])]
        valid_dates += [str(run['oms_attributes']['start_time'])[5:10]]

    
    new_runs = []
    dates = []
    print(valid_runs)
    for run in runs:
        
        try:
            i = valid_runs.index(run)
            print()
            new_runs.append(valid_runs[i])
            dates.append(valid_dates[i])
        except:
            print("skipping run: " + str(run))
    runs = new_runs

    print(runs)

    file_names = []
    for run in runs:
        # #run = run.strip()
        # run_num = int(run)
        # if run_num >= 355100: era = 'Run2022B'
        # if run_num >= 355862: era = 'Run2022C'
        # if run_num >= 357538: era = 'Run2022D'
        # if run_num >= 357900: era = 'Run2022E'
        # if run_num >= 360309: era = 'Run2022F'
        #file_names.append(f'https://cmsweb.cern.ch/dqm/offline/data/browse/ROOT/OfflineData/{year}/{ds}/000{run[:4]}xx/')
        file_names.append(f'https://cmsweb.cern.ch/dqm/offline/data/browse/ROOT/OnlineData/original/000{run[:2]}xxxx/000{run[:4]}xx/DQM_V0001_L1T_R000{run}.root')
        #DQM_V0001_R000{run}__{ds}__{era}-{other_thing}-v1__DQMIO.root

    sess = FuturesSession()
    sess.verify = os.environ["CACERT"]
    sess.cache = os.environ["CACHE"]
    sess.cert = (os.environ["PUBLIC_KEY"], os.environ["PRIVATE_KEY"])


    response = _fetch_dqm_rows(full_path).result()
    with open(f'/root/csctiming/tmp.root', 'wb') as f:
         for chunk in response.iter_content(chunk_size=8192):
             f.write(chunk)
    
    file = uproot.open("/root/csctiming/tmp.root")
    

    hits_bx0 = file["DQMData/Run 355892/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBX0;1"].values()
    hits_bxneg1 = file["DQMData/Run 355892/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBXNeg1;1"].values()
    hits_bxpos1 = file["DQMData/Run 355892/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBXPos1;1"].values()
    del file
    os.remove("/root/csctiming/tmp.root")
    hits_bx0 = np.zeros(hits_bx0.shape)
    hits_bxneg1 = np.zeros(hits_bxneg1.shape)
    hits_bxpos1 = np.zeros(hits_bxpos1.shape)
    plots = []
    threads = []

    final_runs = []
    def process_file(idx, fn):
        nonlocal final_runs
        # nonlocal sess
        sess = FuturesSession()
        sess.verify = os.environ["CACERT"]
        sess.cache = os.environ["CACHE"]
        sess.cert = (os.environ["PUBLIC_KEY"], os.environ["PRIVATE_KEY"])
        # response = sess.get(fn, verify = sess.verify, stream=True).result()
        # soup = BeautifulSoup(response.content, features = "lxml")
        # for tag in soup.findAll("a"):
        #     file = tag["href"]
        #     if runs[idx] in file and "DQMIO" in file:
        #         file_name = file
        #         final_runs += [runs[idx]]
        #         break
        # else: return
    
        # fn += file_name.split("/")[-1]
        # print(fn)

        response = sess.get(fn, verify = sess.verify, stream=True).result()

        with open(f'/root/csctiming/tmp{idx}.root', 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        del response

    
        try:
            nonlocal hits_bx0
            nonlocal hits_bxneg1
            nonlocal hits_bxpos1
            run_num = runs[idx]
            print(f"Run: {run_num}")
            file = uproot.open(f'/root/csctiming/tmp{idx}.root')
            hits_bx0 += file[f"DQMData/Run {run_num}/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBX0;1"].values()
            hits_bxneg1 += file[f"DQMData/Run {run_num}/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBXNeg1;1"].values()
            hits_bxpos1 += file[f"DQMData/Run {run_num}/L1T/Run summary/L1TStage2EMTF/Timing/cscLCTTimingBXPos1;1"].values() 
            del file
            file = ROOT.TFile(f'/root/csctiming/tmp{idx}.root')
            for plot_type in plot_types:
                nonlocal plots
                if plot_type == '': continue
                plot_path = f'DQMData/Run {run_num}/{plot_type}'
                ROOT.gStyle.SetOptStat(0)
                ROOT.gROOT.ForceStyle()                  
                new_plot = file.Get(plot_path)
                new_plot.SetDirectory(0)
                new_plot.Draw()
                new_plot.SetTitle(new_plot.GetTitle() + ", Run #" + str(run_num) + ", Date: " + str(dates[idx]))
                new_plot.SetName(plot_type + "/" + str(run_num))
                print(plot_type)
                
                plots.append(new_plot)
            del file
            final_runs += [runs[idx]]
        except Exception as e: print(e)

                    
        os.remove(f'/root/csctiming/tmp{idx}.root')


    
    # for idx, fn in enumerate(file_names):

    #     x = threading.Thread(target=process_file, args=(fn,idx))
    #     x.start()
    #     threads.append(x)

    # for thread in threads:
    #     while (thread.is_alive()):
    #         time.sleep(.1)
    #         continue

    pool = ThreadPool().imap_unordered(lambda p: process_file(*p), enumerate(file_names))

    for result in pool:
        print(result)
    outfile = ROOT.TFile("/root/csctiming/data.root", 'recreate')
    
    for plot in plots:
        plot.Write()
        del plot

    del outfile

    hits_bx0_noneighbors = np.delete(hits_bx0, [2,9,16,23,30,37], 0)
    hits_bxneg1_noneighbors = np.delete(hits_bxneg1, [2,9,16,23,30,37], 0)
    hits_bxpos1_noneighbors = np.delete(hits_bxpos1, [2,9,16,23,30,37], 0)

    arr_hits_bx0 = np.reshape(hits_bx0_noneighbors, 720, order='F')
    arr_hits_bxneg1 = np.reshape(hits_bxneg1_noneighbors, 720, order='F')
    arr_hits_bxpos1 = np.reshape(hits_bxpos1_noneighbors, 720, order='F')



    station_ring = ['ME-4/2',
    'ME-4/1',
    'ME-3/2',
    'ME-3/1',
    'ME-2/2',
    'ME-2/1',
    'ME-1/3',
    'ME-1/2',
    'ME-1/1b',
    'ME-1/1a',
    'ME+1/1a',
    'ME+1/1b',
    'ME+1/2',
    'ME+1/3',
    'ME+2/1',
    'ME+2/2',
    'ME+3/1',
    'ME+3/2',
    'ME+4/1',
    'ME+4/2']

    inner_station_ring = ['ME-4/1',
    'ME-3/1',
    'ME-2/1',
    'ME+2/1',
    'ME+3/1',
    'ME+4/1']


    # Defining chamber numbers, inner rings have 18 chambers

    chamber = ['1',
            '2',
    #           'N',
            '3',
            '4',
            '5',
            '6',
            '7',
            '8',
    #           'N',
            '9',
            '10',
            '11',
            '12',
            '13',
            '14',
    #           'N',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
    #           'N',
            '21',
            '22',
            '23',
            '24',
            '25',
            '26',
    #           'N',
            '27',
            '28',
            '29',
            '30',
            '31',
            '32',
    #           'N',
            '33',
            '34',
            '35',
            '36'
            ]


    all_names = []

    for idx_station_ring, station_ring_name in enumerate(station_ring):
        for idx_chamber, chamber_number in enumerate(chamber):
            #print(station_ring_name)
            #print(chamber_number)
            if station_ring_name in inner_station_ring: 
                half_chamber_number = str(int(chamber_number)/2)
                #print(half_chamber_number)
                if (idx_chamber % 2) == 0:
                    new_name = station_ring_name + '/' + half_chamber_number
                else:
                    new_name = station_ring_name + '/' + half_chamber_number
                #print(new_name)
                #print(idx_chamber)
            else:
                new_name = station_ring_name + '/' + chamber_number
            all_names.append(new_name)



    df = pd.DataFrame({'Chamber': all_names,
                    'BX-1': arr_hits_bxneg1,
                    'BX0': arr_hits_bx0,
                    'BX+1': arr_hits_bxpos1})


    df_drop_half = (df
    .assign(has_half = lambda x: x['Chamber'].str.contains('\.5'),
            a_or_b = lambda x: x['Chamber'].str.contains('a') | x['Chamber'].str.contains('b'))
    .query('(~has_half) & (~a_or_b)')
    .drop(['has_half', 'a_or_b'], axis=1))



    # Drop the decimals from the names

    subset = (df_drop_half
    .assign(has_point = lambda x: x['Chamber'].str.contains('\.'))
    .query('has_point')
    .assign(Chamber = lambda x: x['Chamber'].str.replace('\.0', ''))
    .assign(new_bx1 = lambda x: 2 * x['BX-1'],
            new_bx0 = lambda x: 2 * x['BX0'],
            new_bxp1 = lambda x: 2 * x['BX+1'])
    [['Chamber', 'new_bx1', 'new_bx0', 'new_bxp1']]
    .rename({'new_bx1': 'BX-1', 'new_bx0': 'BX0', 'new_bxp1': 'BX+1'}, axis=1))

    df_drop_half.loc[subset.index] = subset


    # Create df with ME1/1a chambers

    df1 = df[df["Chamber"].str.contains("a")]

    # Get names of ME1 chambers

    me1_names = df1["Chamber"].str.replace("a", "", regex=True)

    # Create df with ME1/1a chambers

    df2 = df[df["Chamber"].str.contains("b")]

    # Combine ME1/1a and ME1/1b chambers

    me1_bx0 = df1['BX0'].to_numpy() + df2['BX0'].to_numpy()
    me1_bxneg1 = df1['BX-1'].to_numpy() + df2['BX-1'].to_numpy()
    me1_bxpos1 = df1['BX+1'].to_numpy() + df2['BX+1'].to_numpy()

    df_me1 = pd.DataFrame({'Chamber': me1_names,
                    'BX-1': me1_bxneg1,
                    'BX0': me1_bx0,
                    'BX+1': me1_bxpos1})


    df_final = pd.concat([df_drop_half,df_me1])
    del df_drop_half
    del df_me1
    print(df_final)
    html_df = df_final.to_html()
    del df_final
    return html_df, ",".join(final_runs)


