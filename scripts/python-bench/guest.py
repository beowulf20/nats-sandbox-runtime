import os, json, time, hashlib, sys
assert __debug__ and sys.flags.optimize==0
assert not hasattr(sys, "_is_gil_enabled") or sys._is_gil_enabled()
from pathlib import Path
loaded_numpy='numpy' in sys.modules
loaded_pyplot='matplotlib.pyplot' in sys.modules
assert os.statvfs('/').f_flag & os.ST_RDONLY
assert not os.statvfs('/workspace').f_flag & os.ST_RDONLY
assert os.getuid()==65534 and os.getcwd()=='/workspace'
marker=Path('thread-marker')
if marker.exists(): assert marker.read_text()==thread
marker.write_text(thread)
os.makedirs('outputs',exist_ok=True)
started=time.perf_counter()
inputs={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('artifacts').glob('*') if p.is_file()}
payload_bytes=0
import_ms=0
backend=''
extra={"loaded_numpy":loaded_numpy,"loaded_pyplot":loaded_pyplot,"version":sys.version,"optimize":sys.flags.optimize,"debug":__debug__,"jit_available":bool(hasattr(sys,"_jit") and sys._jit.is_available()),"jit_enabled":bool(hasattr(sys,"_jit") and sys._jit.is_enabled())}

if mode=='audit':
    import sys,sysconfig,importlib.util,importlib.metadata,struct,ctypes,glob
    cache={'sources':0,'valid':0,'missing':0,'stale':0,'invalid':0}
    bases=[]
    for path in sorted(set(sys.path),key=len):
        p=Path(path)
        if p.is_absolute() and p.is_dir() and not p.is_relative_to('/workspace') and not any(p.is_relative_to(base) for base in bases): bases.append(p)
    for base in bases:
        for p in Path(base).rglob('*.py'):
            if 'test' in p.parts or 'tests' in p.parts: continue
            cache['sources']+=1
            pyc=Path(importlib.util.cache_from_source(str(p)))
            if not pyc.is_file(): cache['missing']+=1;continue
            header=pyc.read_bytes()[:16]
            if len(header)!=16 or header[:4]!=importlib.util.MAGIC_NUMBER: cache['invalid']+=1;continue
            flags=struct.unpack('<I',header[4:8])[0]
            if flags&1:
                valid=not(flags&2) or header[8:16]==importlib.util.source_hash(p.read_bytes())
            else:
                mtime,size=struct.unpack('<II',header[8:16]);st=p.stat();valid=mtime==(int(st.st_mtime)&0xffffffff) and size==(st.st_size&0xffffffff)
            cache['valid' if valid else 'stale']+=1
    os.environ['MPLCONFIGDIR']='/workspace/artifacts/mplconfig'
    os.environ['MPLBACKEND']='Agg'
    import numpy as np
    pools=[]
    for lib in sorted((Path(np.__file__).resolve().parent.parent/'numpy.libs').glob('*openblas*')):
        dll=ctypes.CDLL(str(lib))
        for name in ['scipy_openblas_get_num_threads64_','scipy_openblas_get_num_threads','openblas_get_num_threads64_','openblas_get_num_threads']:
            try: f=getattr(dll,name)
            except AttributeError: continue
            f.restype=ctypes.c_int;pools.append({'library':str(lib),'threads':f()});break
    extra.update(version=sys.version,config={k:sysconfig.get_config_var(k) for k in ['CONFIG_ARGS','PY_CFLAGS','PY_CFLAGS_NODIST','PGO_PROF_USE_FLAG','Py_GIL_DISABLED','Py_TAIL_CALL_INTERP','SOABI']},cache=cache,pools=pools,cpu_count=os.cpu_count(),packages={p.metadata['Name']:p.version for p in importlib.metadata.distributions()})
if mode=='cpu':
    def compute(n):
        total=0
        for i in range(n): total=(total+i*i)%1000000007
        return total
    t=time.perf_counter();value=compute(3000000);extra['cpu_ms']=(time.perf_counter()-t)*1000
    assert value==531941, value
    source='def f(x):\n return x*x+1\n'+'\n'.join('v%d=f(%d)'%(i,i) for i in range(1000))
    t=time.perf_counter()
    for _ in range(40): compile(source,'<benchmark>','exec')
    extra['compile_ms']=(time.perf_counter()-t)*1000
if mode in ('blas','blas1'):
    if mode=='blas1':
        os.environ['OPENBLAS_NUM_THREADS']='1';os.environ['OMP_NUM_THREADS']='1'
    t=time.perf_counter();import numpy as np
    import_ms=(time.perf_counter()-t)*1000
    a=np.arange(640*640,dtype=np.float64).reshape(640,640)/1000
    t=time.perf_counter()
    for _ in range(5): b=a@a.T
    extra['blas_ms']=(time.perf_counter()-t)*1000
    assert np.isclose(b[0,0],sum((i/1000)**2 for i in range(640)),rtol=1e-12)

if mode=='state':
    import numpy as np, random
    assert not hasattr(np,'_prior_user')
    np._prior_user=thread
    extra['random_values']=[random.random(),float(np.random.random()),float(np.random.default_rng().random()),os.urandom(16).hex()]
    random.seed(42);np.random.seed(42)
    assert random.random()==0.6394267984578837 and np.isclose(np.random.random(),0.3745401188473625)
    if profile in ('plot-agg','plot-cairo'):
        import matplotlib.pyplot as plt
        assert not plt.get_fignums()
        assert plt.rcParams['lines.linewidth']==1.5
        plt.rcParams['lines.linewidth']=33
        plt.figure()
if mode=='backend':
    import importlib
    module='caller_module_'+str(run_id)
    Path(module+'.py').write_text('VALUE='+repr(thread)+'\n')
    sys.path.insert(0,'/workspace')
    assert importlib.import_module(module).VALUE==thread
    os.environ['MPLCONFIGDIR']='/workspace/artifacts/mplconfig'
    os.environ['MPLBACKEND']='svg'
    import matplotlib.pyplot as plt
    backend=plt.get_backend()
if mode=='io':
    chunk=b'x'*65536
    with open('outputs/payload.bin','wb') as out:
        for _ in range(128): out.write(chunk)
    payload_bytes=8*1024*1024
if mode=='plot':
    selected_backend='Agg' if renderer=='agg' else 'module://mplcairo.base'
    os.environ['MPLBACKEND']=selected_backend
    if profile not in ('plot-agg','plot-cairo'): os.environ['MPLCONFIGDIR']='/workspace/artifacts/mplconfig'
    t=time.perf_counter()
    import matplotlib.pyplot as plt
    import_ms=(time.perf_counter()-t)*1000
    import numpy as np, importlib.metadata
    if renderer=='cairo':
        import cairo, mplcairo
        extra.update(cairo_version=cairo.cairo_version_string(),mplcairo_version=importlib.metadata.version('mplcairo'),cairo_options={k:str(v) for k,v in mplcairo.get_options().items()})
    extra.update(numpy_version=np.__version__,matplotlib_version=importlib.metadata.version('matplotlib'))
    assert plt.get_backend().lower()==selected_backend.lower()
    rng=np.random.default_rng(12345)
    if plot_type=='scatter':
        x=rng.normal(size=5000);y=0.4*x+rng.normal(scale=0.5,size=5000)
    elif plot_type=='dense':
        x=np.linspace(0,20,100000);y=np.sin(x)*np.cos(3*x)+0.05*np.sin(150*x)
    else:
        x=np.linspace(0,6*np.pi,2000);y=np.sin(x)+0.2*np.cos(3*x)
    data_hash=hashlib.sha256(np.column_stack((x,y)).astype('<f8').tobytes()).hexdigest()
    axes_ms=save_ms=draw_ms=0
    titles=[]
    for chart in range(charts):
        t=time.perf_counter()
        fig,ax=plt.subplots(figsize=(6.4,4.8),dpi=100)
        if plot_type=='scatter': ax.scatter(x,y,s=12,alpha=0.65,color='#1f77b4')
        else: ax.plot(x,y,color='#1f77b4')
        title=plot_type.capitalize()+' / seed 12345'
        ax.set(title=title,xlabel='Input',ylabel='Value')
        assert len(ax.collections if plot_type=='scatter' else ax.lines)==1
        assert ax.get_xlabel()=='Input' and ax.get_ylabel()=='Value' and ax.get_title()==title
        titles.append(title)
        axes_ms+=(time.perf_counter()-t)*1000
        draw_times=[]
        original_draw=fig.draw
        def measured_draw(target_renderer):
            draw_start=time.perf_counter()
            try: return original_draw(target_renderer)
            finally: draw_times.append((time.perf_counter()-draw_start)*1000)
        fig.draw=measured_draw
        t=time.perf_counter()
        fig.savefig('outputs/chart-'+str(chart)+'.png',pil_kwargs={'compress_level':compression})
        save_ms+=(time.perf_counter()-t)*1000
        assert draw_times
        draw_ms+=sum(draw_times)
        plt.close(fig)
    extra.update(axes_ms=axes_ms,save_ms=save_ms,draw_ms=draw_ms,png_other_ms=save_ms-draw_ms,
                 plot_type=plot_type,points=len(x),data_hash=data_hash,titles=titles)
    extra['plot_backend']=plt.get_backend()
stats={}
for line in open('/proc/self/status'):
    key,_,value=line.partition(':')
    if key in ('VmRSS','VmHWM','VmSwap'): stats[key+'_kib']=int(value.split()[0])
for line in open('/proc/vmstat'):
    key,value=line.split()
    if key in ('pgmajfault','pswpin','pswpout'): stats[key]=int(value)
stats.update(extra)
stats.update(backend=backend,input_hashes=inputs,payload_bytes=payload_bytes,import_ms=import_ms,processing_ms=(time.perf_counter()-started)*1000)
Path('outputs/metrics.json').write_text(json.dumps(stats))
