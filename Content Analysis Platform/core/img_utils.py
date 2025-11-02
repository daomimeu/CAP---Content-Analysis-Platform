import pandas as pd

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

from PIL import Image
from io import BytesIO


def get_img_from_dict(data_dict, storage_client, bucket_name):
    bucket = storage_client.get_bucket(bucket_name)
    
    img_dict = {}
    for cid, data in data_dict.items():
        try:
            if bucket_name == 'creative-edm':
                blob = bucket.get_blob(f"{data['country']}/{cid}.jpg")
            else:
                blob = bucket.get_blob(f"phone/display/{data['country']}/{cid}.jpg")
                if blob is None:
                    blob = bucket.get_blob(f"tablet/display/{data['country']}/{cid}.jpg")

            img = Image.open(BytesIO(blob.download_as_bytes()))
            img_dict[cid] = img
        except:
            continue

    return img_dict


def ctr_adjust_for_color(click_rate_list):
    adjusted_list = []
    for cr in click_rate_list:
        if cr > 0.03:
            adjusted_list.append(1)
        else:
            adjusted_list.append(cr / 0.03)
    return adjusted_list


def color_gradient(click_rate):
    #https://rgbcolorpicker.com/0-1

    r,b = 1 - click_rate, 1 - click_rate
    g = 1 - ((1 - 0.7)*click_rate)

    return (r,g,b)


def draw_click_rate_bar(img, data, click_data_type):
    # https://stackoverflow.com/questions/27267305/plotting-rectangles-in-different-subplots-in-python

    # data is a dict {'country':'sg', 'curiosity':0.4, 'pod_count':3, 'click_rate':[0.3, 0.2, 0.4], ...}

    w, h = img.size
    h = h*1.05 #add 5% for footer

    pod_count = data['pod_count']

    if click_data_type == 'Pod click contribution':
        click_rate_list = data['click_rate']
        color_data_list = data['click_rate']
        round_decimal = 1
    elif click_data_type == 'Pod CTR':
        click_rate_list = data['pod_ctr']
        color_data_list = ctr_adjust_for_color(click_rate_list)
        round_decimal = 2

    click_label_list = data['label_name']
    height_ratios = data['height']

    color = []
    click_rate = []
    click_label = []
    for i in range(pod_count):
        color.append(color_gradient(color_data_list[i])) #convert click rate to color
        click_rate.append(str(round(click_rate_list[i]*100, round_decimal)) + '%') #display click rate as percentage
        click_label.append(str(click_label_list[i]))

    df = pd.DataFrame({
        'Color': color,
        'Value': click_rate,
        'Label': click_label
    })

    px = 1/plt.rcParams['figure.dpi'] #pixel in inches
    fig = plt.figure(figsize=(75*px, 300*h/w*px)) #image width is fixed at 300px on CAP --> calc corresponding height
    ax = fig.subplots(pod_count, 1, height_ratios=height_ratios)

    for i in range(pod_count):
        ax[i].add_patch(Rectangle(xy=(0,0), width=100, height=100, facecolor=df['Color'][i], edgecolor='black')) #arbitrarily pick 100 for scale
        ax[i].annotate(df['Value'][i], xy=(50, 60), ha='center', va='center', color='black', fontsize=8.5) #annotate click rate in middle
        ax[i].annotate(df['Label'][i], xy=(50, 40), ha='center', va='center', color='black', fontsize=7) #annotate label name in middle

        ax[i].set_ylim((0,100))
        ax[i].set_xlim((0,100))
        ax[i].set_frame_on(False) #remove frame
        ax[i].set_axis_off() #remove axis

    plt.tight_layout() #remove white space
    plt.subplots_adjust(wspace=0, hspace=0) #remove gap between axs

    # convert fig to image object
    buf = BytesIO()
    fig.savefig(buf)
    buf.seek(0)
    img_out = Image.open(buf)

    return img_out


def truncate_labels(labels, max_len=10): #labels is a list
    truncated_labels = []
    for label in labels:
        if len(label) > max_len:
            truncated_label = label[:max_len-2] + '..'
        else:
            truncated_label = label
        truncated_labels.append(truncated_label)
    return truncated_labels