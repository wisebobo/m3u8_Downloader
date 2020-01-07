# -*- coding:utf-8 -*-
from multiprocessing import Pool, Manager
from urllib.parse import urljoin
import m3u8
import os, sys, requests, time
import queue, threading

class m3u8_Downloader:

    def __init__(self, uri, outDir, outName, type=1, no=1):
        self.uri = uri
        self.outDir = outDir
        self.outName = outName

        if type in (1, 2):

            if outDir and not os.path.isdir(outDir):
                os.makedirs(outDir)

            self.m3u8 = m3u8.load(uri=uri, timeout=10)

            if self.m3u8:
                self.ts_count = 0
                self.ts_total = len(self.m3u8.files)

                self.session = self.get_session(50, 50, 10)

                print('Total ts count = ' + str(self.ts_total))

                # Multiple threads
                if type == 1:
                    self.q_lock = threading.Lock()
                    self.q = queue.Queue(self.ts_total)

                    for index, ts_file in enumerate(self.m3u8.files):
                        self.q.put(ts_file)

                    self.download_by_thread(no_of_thread=no)
                # Multiple processes
                elif type == 2:
                    self.download_by_process(no_of_process=no)

    def get_session(self, pool_connections, pool_maxsize, max_retries):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=max_retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def download_by_process(self, no_of_process=50):

        def download(ts_file, ts_count, process_lock):
            retry = 3
            while retry:
                try:
                    url = urljoin(self.m3u8.base_uri, ts_file)
                    r = self.session.get(url, timeout=20)
                    if r.ok:
                        outFileName = os.path.join(self.outDir, ts_file)
                        with open(outFileName, 'wb') as f:
                            f.write(r.content)

                        process_lock.acquire()
                        ts_count.value = ts_count.value + 1
                        process_lock.release()
                        self.show_progress(ts_count.value / self.ts_total)
                        break
                except Exception as e:
                    print(e)
                    retry -= 1

        print('\nStart to download')
        pool = Pool(processes=no_of_process)
        ts_count = Manager().Value('i', 0)
        process_lock = Manager().Lock()

        for index, ts_file in enumerate(self.m3u8.files):
            _ = pool.apply_async(download, (ts_file, ts_count, process_lock))

        pool.close()
        pool.join()

        print('\nStart to merge')
        self.merge_file()


    def download_by_thread(self, no_of_thread=50):

        def download():
            while True:
                if not self.q.empty():
                    ts_file = self.q.get()
                    retry = 3
                    while retry:
                        try:
                            url = urljoin(self.m3u8.base_uri, ts_file)
                            r = self.session.get(url, timeout=20)
                            if r.ok:
                                outFileName = os.path.join(self.outDir, ts_file)
                                with open(outFileName, 'wb') as f:
                                    f.write(r.content)

                                self.ts_count = self.ts_count + 1
                                self.show_progress(self.ts_count / self.ts_total)
                                break
                        except Exception as e:
                            print(e)
                            retry -= 1
                else:
                    break

        print('\nStart to download')
        threads = []
        for _ in range(no_of_thread):
            threads.append(threading.Thread(target=download))

        for t in threads: t.start()
        for t in threads: t.join()

        print('\nStart to merge')
        self.merge_file()

    def show_progress(self, percent):
        bar_length=50
        hashes = '#' * int(percent * bar_length)
        spaces = ' ' * (bar_length - len(hashes))
        sys.stdout.write("\rPercent: [%s] %.2f%%"%(hashes + spaces, percent*100))
        sys.stdout.flush()

    def merge_file(self):
        outfile = ''
        outFileName = os.path.join(self.outDir, self.outName + '.mp4')

        for index, ts_file in enumerate(self.m3u8.files):
            inFileName = os.path.join(self.outDir, ts_file)

            percent = (index + 1) / self.ts_total
            self.show_progress(percent)
            if not outfile:
                outfile = open(outFileName, 'wb')

            with open(inFileName, 'rb') as infile:
                    outfile.write(infile.read())

            os.remove(inFileName)

        if outfile:
            outfile.close()



if __name__ == '__main__':
    m3u8_obj = m3u8_Downloader(uri='https://youku.cdn4-okzy.com/20200106/3620_6970c056/1000k/hls/index.m3u8',
                               outDir='E:/Python/MyProgram/temp/m38u_download/',
                               outName='丫鬟大联盟粤语06',
                               type=1,
                               no=50)
