from visual import *
from visual.graph import *

gd = gdisplay(x=300, y=0, width=600, height=600,
    title='Entropy', xtitle='time', ytitle='N',
    foreground=color.black, background=color.white,
    xmax=250, xmin=0, ymax=400, ymin=0.)

funct1=gcurve(color=color.black)
funct2=gcurve(color=color.black)

for i in range(0,200):
    funct1.plot(pos=(i,i*2))
    funct2.plot(pos=(i,400-i*2))
    rate(20)
