################
# PLACEHOLDERS #
################
# dataframe2 := dataframe contains: | i | D[i] | D[i-1] | f[i] | f[i-1] | close[i] | open[i] |... for i in [1,n]

# part 5

# helper function =2u(t)-1 where u is heaviside step
heavi2 = lambda x: 1 if x>0 else -1

# These values are needed nowhere else; i'm not going to bother computing them

#dR_tdf_t = heavi2(f[t-1]-f[t])
#dR_tdf_t_1 = close[t] - open[t] -dR_tdf_t
#df_t_1dtheta = D_t_1
#df_tdtheta = D_t
#dU_tdtheta=sum((dR_tdf_t)*(df_tdtheta) + (dR_tdf_t_1)*(df_t_1dtheta)))

# $\frac{dU_T}{d\theta}=\sum_{t=1}^T \frac{dU_T}{dR_t} ((\frac{dR_t}{df_t})*(\frac{df_t}{d\theta}) + (\frac{dR_t}{df_{t-1}})*(\frac{df_{t-1}}{dtheta})) $
dUdtheta = dataframe2.map(lambda x: x['dU[i]/dR[i]']*(heavi2(x['f[i-1]']-x['f[i]'])*x['D[i]'] + (x['close[i]']-x['open[i]']-heavi2(x['f[i-1]']-x['f[i]']))*x['D[i-1]']))
