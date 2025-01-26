import scrapy
from scrapy.spiders import SitemapSpider
from urllib.parse import urlparse, urldefrag
import json

class LinkCollectorSpider(SitemapSpider):
    name = 'link_collector'
    
    def __init__(self, domain=None, *args, **kwargs):
        super(LinkCollectorSpider, self).__init__(*args, **kwargs)
        self.domain = domain
        self.sitemap_urls = [f'https://{domain}/sitemap.xml']
        self.allowed_domains = [domain]
        self.visited_urls = set()
        self.links_file = f"{self.domain}_links.jsonl"
    
    def parse(self, response):
        current_url = response.url
        if current_url in self.visited_urls:
            return
        self.visited_urls.add(current_url)
        
        for link in response.css('a::attr(href)').getall():
            absolute_url = response.urljoin(link)
            # Remove fragments and query params
            clean_url = urldefrag(absolute_url)[0].split('?')[0]
            parsed_url = urlparse(clean_url)
            
            if parsed_url.netloc == self.domain and clean_url not in self.visited_urls:
                self.visited_urls.add(clean_url)
                with open(self.links_file, 'a', encoding='utf-8') as file:
                    file.write(json.dumps({'url': clean_url}) + '\n')
                yield scrapy.Request(clean_url, callback=self.parse)
    
    def closed(self, reason):
        # Deduplicate the collected links
        with open(self.links_file, 'r', encoding='utf-8') as file:
            links = {json.loads(line)['url'] for line in file}
        
        with open(self.links_file, 'w', encoding='utf-8') as file:
            for link in links:
                file.write(json.dumps({'url': link}) + '\n')
        self.logger.info(f"Deduplicated links saved to {self.links_file}")
