def scan_mappy(lst):
    z = list(map(lambda x: ([x],x),lst))
    # each iteration, we group into tuples of tuples, then combine
    while len(z)>1:
        # group into tuples: we couldn't figure out how to do this with spark
        zg=[]
        for i in range(int(len(z)/2)):
            zg.append((z[2*i],z[2*i+1]))
        if len(zg)<.5*len(z):
            zg.append((z[len(z)-1],([],0)))
        # now map groups together: this is easy to spark-ify.
        z=list(map(lambda x: (x[0][0]+list(map( lambda y:y+x[0][1], x[1][0])), x[0][1]+x[1][1]) ,zg))
    return z[0][0]
