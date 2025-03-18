#%%
import os
import requests
from urllib.parse import urljoin
import concurrent.futures
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import asyncio
import aiohttp
import aiofiles
import logging
from functools import partial
#%%
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('embrace_download.log')]
)
logger = logging.getLogger('embrace_downloader')

class EmbraceDataDownloader:
    """Class to download magnetometer data from EMBRACE/INPE with high-performance parallel downloads."""
    
    def __init__(self, base_url="https://embracedata.inpe.br/.mag_seg/", years=None, stations=None, output_dir="embrace_data"):
        """
        Initialize the downloader with the parameters.
        
        Args:
            base_url (str): The root URL for the data
            years (list): List of years to download (e.g. ['2022', '2023', '2024'])
            stations (list): List of station codes (three letters) to download.
                            If None, will attempt to discover all available stations.
            output_dir (str): Base directory for downloaded files
        """
        self.base_url = base_url
        self.years = years or ['2022', '2023', '2024']
        self.stations = stations
        self.output_dir = output_dir
        self.session = requests.Session()
        # Configure session for better performance
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Connection': 'keep-alive'
        })
        self.semaphore = None  # Will be initialized in async methods
        
    async def get_available_stations_async(self, year, session):
        """Get a list of available stations for a given year using async."""
        url = urljoin(self.base_url, f"{year}/")
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Failed to get stations for year {year}: HTTP {response.status}")
                    return []
                
                html = await response.text()
                # Parse the HTML to find directories (stations)
                soup = BeautifulSoup(html, 'html.parser')
                stations = []
                
                for link in soup.find_all('a'):
                    href = link.get('href')
                    if href and len(href) == 4 and href.endswith('/') and href.lower() != '../':
                        stations.append(href[:-1])  # Remove trailing slash
                        
                logger.info(f"Found {len(stations)} stations for year {year}")
                return stations
        except Exception as e:
            logger.error(f"Error getting stations for year {year}: {e}")
            return []
    
    async def get_files_for_station_async(self, year, station, session):
        """Get a list of available zip files for a station in a given year using async."""
        url = urljoin(self.base_url, f"{year}/{station}/")
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Failed to get files for station {station} in year {year}: HTTP {response.status}")
                    return []
                
                html = await response.text()
                # Parse the HTML to find zip files
                soup = BeautifulSoup(html, 'html.parser')
                files = []
                
                for link in soup.find_all('a'):
                    href = link.get('href')
                    if href and href.endswith('.zip'):
                        files.append(href)
                
                logger.info(f"Found {len(files)} files for station {station} in year {year}")
                return files
        except Exception as e:
            logger.error(f"Error getting files for station {station} in year {year}: {e}")
            return []
    
    async def download_file_async(self, year, station, filename, session, output_dir=None, pbar=None):
        """Download a specific file using aiohttp."""
        async with self.semaphore:  # Limit concurrent downloads
            file_url = urljoin(self.base_url, f"{year}/{station}/{filename}")
            
            if output_dir is None:
                output_dir = os.path.join(self.output_dir, year, station)
            
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            output_path = os.path.join(output_dir, filename)
            
            # Skip download if file already exists
            if os.path.exists(output_path):
                logger.info(f"File already exists: {output_path}")
                if pbar:
                    pbar.update(1)
                return True
            
            try:
                retries = 3
                for attempt in range(retries):
                    try:
                        async with session.get(file_url) as response:
                            if response.status != 200:
                                logger.error(f"Failed to download {filename} (attempt {attempt+1}/{retries}): HTTP {response.status}")
                                if attempt < retries - 1:
                                    await asyncio.sleep(1)  # Wait before retrying
                                    continue
                                return False
                            
                            # Create temp file path
                            temp_path = f"{output_path}.part"
                            
                            # Download the file
                            async with aiofiles.open(temp_path, 'wb') as f:
                                chunk_size = 64 * 1024  # 64KB chunks
                                async for chunk in response.content.iter_chunked(chunk_size):
                                    await f.write(chunk)
                            
                            # Rename temp file to final file
                            os.rename(temp_path, output_path)
                            
                            # Update progress bar
                            if pbar:
                                pbar.update(1)
                            
                            logger.info(f"Downloaded {filename} for station {station} in year {year}")
                            return True
                    except aiohttp.ClientError as e:
                        logger.warning(f"Connection error downloading {filename} (attempt {attempt+1}/{retries}): {e}")
                        if attempt < retries - 1:
                            await asyncio.sleep(2)  # Wait before retrying
                        else:
                            raise
                
                return False  # All retries failed
            except Exception as e:
                logger.error(f"Error downloading {filename} for station {station} in year {year}: {e}")
                # Remove partial download
                if os.path.exists(f"{output_path}.part"):
                    os.remove(f"{output_path}.part")
                return False
    
    async def process_station_async(self, year, station, session, max_concurrent_downloads, progress_manager):
        """Process all files for a station asynchronously."""
        logger.info(f"Processing station {station} for year {year}")
        
        files = await self.get_files_for_station_async(year, station, session)
        
        if not files:
            logger.warning(f"No files found for station {station} in year {year}")
            return 0
        
        # Create a progress bar for this station
        station_output_dir = os.path.join(self.output_dir, year, station)
        
        # Submit all download tasks
        download_tasks = []
        for filename in files:
            download_tasks.append(
                self.download_file_async(
                    year, 
                    station, 
                    filename, 
                    session, 
                    station_output_dir,
                    progress_manager.main_pbar
                )
            )
        
        # Wait for all downloads to complete
        results = await asyncio.gather(*download_tasks)
        
        # Count successful downloads
        successful = sum(1 for r in results if r)
        logger.info(f"Downloaded {successful}/{len(files)} files for station {station} in year {year}")
        
        return successful
    
    async def process_year_async(self, year, max_concurrent_downloads, max_concurrent_stations, progress_manager):
        """Process all stations for a year asynchronously."""
        logger.info(f"Processing year {year}")
        
        # Use a session timeout and connection limit for better performance
        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_connect=30, sock_read=30)
        connector = aiohttp.TCPConnector(limit=max_concurrent_stations*2, ttl_dns_cache=300)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            # Get stations if not specified
            stations_to_download = self.stations
            if not stations_to_download:
                stations_to_download = await self.get_available_stations_async(year, session)
                logger.info(f"Discovered stations for {year}: {stations_to_download}")
            
            if not stations_to_download:
                logger.warning(f"No stations found for year {year}")
                return 0
            
            # Create station processing tasks
            station_tasks = []
            station_semaphore = asyncio.Semaphore(max_concurrent_stations)
            
            for station in stations_to_download:
                # Use a semaphore to limit the number of concurrent station processes
                async def process_station_with_semaphore(station):
                    async with station_semaphore:
                        return await self.process_station_async(
                            year, station, session, max_concurrent_downloads, progress_manager
                        )
                
                station_tasks.append(process_station_with_semaphore(station))
            
            # Wait for all station tasks to complete
            station_results = await asyncio.gather(*station_tasks)
            
            total_files_downloaded = sum(station_results)
            logger.info(f"Year {year} completed. Total files downloaded: {total_files_downloaded}")
            
            return total_files_downloaded
    
    class ProgressManager:
        """Manage progress bars for the download process."""
        def __init__(self, total_tasks):
            self.main_pbar = tqdm(total=total_tasks, desc="Total progress", unit="file")
        
        def close(self):
            self.main_pbar.close()
    
    async def download_all_async(self, max_concurrent_downloads=20, max_concurrent_stations=5, max_concurrent_years=3):
        """Download all data for the specified years and stations using asyncio."""
        logger.info(f"Starting download with concurrency settings: "
                   f"downloads={max_concurrent_downloads}, "
                   f"stations={max_concurrent_stations}, "
                   f"years={max_concurrent_years}")
        
        # Create base output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize semaphore for limiting concurrent downloads
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)
        
        # Create year processing tasks
        year_tasks = []
        year_semaphore = asyncio.Semaphore(max_concurrent_years)
        
        # Initialize progress manager with a placeholder total (we'll update it later)
        progress_manager = self.ProgressManager(100)
        
        start_time = time.time()
        
        try:
            for year in self.years:
                async def process_year_with_semaphore(year):
                    async with year_semaphore:
                        return await self.process_year_async(
                            year, max_concurrent_downloads, max_concurrent_stations, progress_manager
                        )
                
                year_tasks.append(process_year_with_semaphore(year))
            
            # Wait for all year tasks to complete
            year_results = await asyncio.gather(*year_tasks)
            
            # Log summary
            total_files = sum(year_results)
            elapsed_time = time.time() - start_time
            logger.info(f"Download completed. Total files: {total_files}, Time: {elapsed_time:.1f}s, "
                       f"Average speed: {total_files/elapsed_time:.2f} files/s")
            
            return total_files
        finally:
            # Close progress bars
            progress_manager.close()
    
    def download_all(self, max_concurrent_downloads=20, max_concurrent_stations=5, max_concurrent_years=3):
        """Synchronous wrapper for the async download function."""
        logger.info("Starting download process")
        
        # Get the event loop or create one if it doesn't exist
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        try:
            return loop.run_until_complete(
                self.download_all_async(max_concurrent_downloads, max_concurrent_stations, max_concurrent_years)
            )
        except KeyboardInterrupt:
            logger.warning("Download interrupted by user")
            # Graceful shutdown
            tasks = asyncio.all_tasks(loop)
            for task in tasks:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            raise
        finally:
            if loop.is_running():
                loop.close()
#%%
# Example usage
if __name__ == "__main__":
    # Initialize the downloader with your preferred settings
    downloader = EmbraceDataDownloader(
        # Base URL for the data
        base_url="https://embracedata.inpe.br/.mag_seg/",
        
        # Years to download - you can modify this list
        years=['2022', '2023', '2024'],
        
        # Stations to download - set to None to discover all stations
        # Or provide a list like ['med', 'sjc', 'eus'] for specific stations
        stations=None,
        
        # Directory where files will be saved
        output_dir="/Users/jose/mag_data/embrace/second_raw"
    )
    
    # Start the download process with parallel processing settings
    # You can adjust these numbers based on your internet connection and computer capabilities
    try:
        downloader.download_all(
            # Maximum number of concurrent file downloads
            max_concurrent_downloads=30,
            
            # Maximum number of stations to process in parallel
            max_concurrent_stations=8,
            
            # Maximum number of years to process in parallel
            max_concurrent_years=3
        )
        print("Download completed successfully!")
    except KeyboardInterrupt:
        print("\nDownload interrupted by user. Partial data has been downloaded.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        logger.error(f"Download failed: {e}", exc_info=True)