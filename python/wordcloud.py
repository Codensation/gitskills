# -*- coding: utf-8 -*-
"""
Created on Tue Oct  9 16:52:49 2018

@author: xn088969
"""
import numpy as np
import os
import jieba
from os import path
from scipy.misc import imread #加载png格式图片所用
from PIL import Image #加载jpg格式图片所用
import matplotlib.pyplot as plt
from wordcloud import WordCloud,STOPWORDS,ImageColorGenerator
def makewordcloud(dir,textname,figurename):
    os.chdir(dir)
    text = open(textname).read()
    wl=" ".join(jieba.cut(text)) #结巴分词
    mask_coloring = imread(figurename) # 背景图片
    # mask_coloring = np.array(Image.open("2.jpg"))
    wc = WordCloud(background_color="white",mask=mask_coloring,
                   max_words=2000,#最大显示的字数
                   #stopwords="",#设置停用词
                   font_path="C:/Windows/Fonts/simfang.ttf",
                   max_font_size=42,scale=1.5,random_state=30) #设置中文字体，需要查找自己的fonts文件,random_state设置多少种配色方案,scale输入词云大小是原来是1.5倍
    mywords = wc.generate(wl)  # 生成词云,可以用generate输入全部文本(中文不好分词),也可以我们计算好词频后使用generate_from_frequencies函数
    # wc.generate_from_frequencies(txt_freq)
    #其中txt_freq例子为[('词a', 100),('词b', 90),('词c', 80)]
    image_colors = ImageColorGenerator(mask_coloring) # 从背景图片生成颜色值
    plt.figure()   # 绘制词云
    plt.imshow(wc.recolor(color_func=image_colors),interpolation="bilinear")
    plt.axis("off")
    return wc.to_file(path.join(d, "ciyun4.png")) # 保存图片
if __name__ == "__main__":
    dir='C:/Users/xn088969/Desktop/'
    textname='大国大城.txt'
    figurename='大国大城.png'
    makewordcloud(dir,textname,figurename)
