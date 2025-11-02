
def get_product_from_model(model):
    if model == 'MX':
        return 'MX'
    

def parse_campaign_id(campaign_id):
    if ',' in campaign_id:
        campaign_list = campaign_id.replace(' ', '').split(',')
    else:
        campaign_list = campaign_id.split(' ')
    
    if not campaign_list[0].startswith('0'):
        campaign_list = ['0000' + id for id in campaign_list]
    
    return campaign_list


def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return '%.1f%s' % (num, ['', 'K', 'M', 'G', 'T', 'P'][magnitude])