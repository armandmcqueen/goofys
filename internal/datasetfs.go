package internal

import (
	"fmt"
	. "github.com/armandmcqueen/goofys/api/common"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sirupsen/logrus"
	"io"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"bufio"
	"encoding/csv"
	"os"

	"context"
)

type ManifestEntry struct {
	Path   string `json:"path"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

var dfsLog = GetLogger("DatasetFS")


func loadManifest() []ManifestEntry {
	dfsLog.Info("Loading test manifest")

	csvFile, _ := os.Open("dataset_fs/manifest.csv")
	reader := csv.NewReader(bufio.NewReader(csvFile))

	var manifest []ManifestEntry
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		manifest = append(manifest, ManifestEntry{
			Path:   line[0],
			Bucket: line[1],
			Key:    line[2],
		})
	}
	//manifestJson, _ := json.MarshalIndent(manifest, "", "    ")
	//fmt.Println(string(manifestJson))

	return manifest
}



type DatasetFS struct {
	fuseutil.NotImplementedFileSystem

	bucket string

	manifest []ManifestEntry


	umask uint32

	rootAttrs InodeAttributes

	bufferPool *BufferPool

	// A lock protecting the state of the file system struct itself (distinct
	// from per-inode locks). Make sure to see the notes on lock ordering above.
	mu sync.RWMutex

	// The next inode ID to hand out. We assume that this will never overflow,
	// since even if we were handing out inode IDs at 4 GHz, it would still take
	// over a century to do so.
	//
	// GUARDED_BY(mu)
	nextInodeID fuseops.InodeID

	// The collection of live inodes, keyed by inode ID. No ID less than
	// fuseops.RootInodeID is ever used.
	//
	// INVARIANT: For all keys k, fuseops.RootInodeID <= k < nextInodeID
	// INVARIANT: For all keys k, inodes[k].ID() == k
	// INVARIANT: inodes[fuseops.RootInodeID] is missing or of type inode.DirInode
	// INVARIANT: For all v, if IsDirName(v.Name()) then v is inode.DirInode
	//
	// GUARDED_BY(mu)
	inodes map[fuseops.InodeID]*Inode

	nextHandleID fuseops.HandleID
	dirHandles   map[fuseops.HandleID]*DirHandle

	fileHandles map[fuseops.HandleID]*FileHandle

	replicators *Ticket
	restorers   *Ticket

	forgotCnt uint32
}






func NewDatasetFS(ctx context.Context, manifest []ManifestEntry) *DatasetFS {
	cloud, err := NewManifestS3()
	if err != nil {
		panic(fmt.Sprintf("Error when creating DatasetFS struct: %s", err))
	}
	// Set up the basic struct.
	fs := &DatasetFS{
		umask:  0122,
	}

	// TODO: Build better data structure than array of tuples?



	// TODO: Go back to original version where this is set by CLI flags
	s3Log.Level = logrus.DebugLevel


	// TODO: Probably makes sense to implement manifest functionality as a new Backend?
	//cloud, err := NewBackend(bucket, flags)
	//if err != nil {
	//	log.Errorf("Unable to setup backend: %v", err)
	//	return nil
	//}

	// TODO: Sample one key and test if we have perms. Unclear what we should do longer-term
	//randomObjectName := prefix + (RandStringBytesMaskImprSrc(32))
	//err = cloud.Init(randomObjectName)
	//if err != nil {
	//	log.Errorf("Unable to access '%v': %v", bucket, err)
	//	return nil
	//}

	now := time.Now()
	fs.rootAttrs = InodeAttributes{
		Size:  4096,
		Mtime: now,
	}

	fs.bufferPool = BufferPool{}.Init()

	fs.nextInodeID = fuseops.RootInodeID + 1
	fs.inodes = make(map[fuseops.InodeID]*Inode)
	root := NewInode(fs, nil, PString(""))
	root.Id = fuseops.RootInodeID
	root.ToDir()
	root.dir.cloud = cloud
	root.dir.mountPrefix = ""
	root.Attributes.Mtime = fs.rootAttrs.Mtime

	fs.inodes[fuseops.RootInodeID] = root
	fs.addDotAndDotDot(root)

	fs.nextHandleID = 1
	fs.dirHandles = make(map[fuseops.HandleID]*DirHandle)

	fs.fileHandles = make(map[fuseops.HandleID]*FileHandle)

	fs.replicators = Ticket{Total: 16}.Init()
	fs.restorers = Ticket{Total: 20}.Init()

	return fs
}




func (fs *DatasetFS) mount(mp *Inode, b *Mount) {
	if b.mounted {
		return
	}

	name := strings.Trim(b.name, "/")

	// create path for the mount. AttrTime is set to TIME_MAX so
	// they will never expire and be removed. But DirTime is not
	// so we will still consult the underlining cloud for listing
	// (which will then be merged with the cached result)

	for {
		idx := strings.Index(name, "/")
		if idx == -1 {
			break
		}
		dirName := name[0:idx]
		name = name[idx+1:]

		mp.mu.Lock()
		dirInode := mp.findChildUnlocked(dirName)
		if dirInode == nil {
			fs.mu.Lock()

			dirInode = NewInode(fs, mp, &dirName)
			dirInode.ToDir()
			dirInode.AttrTime = TIME_MAX

			fs.insertInode(mp, dirInode)
			fs.mu.Unlock()
		}
		mp.mu.Unlock()
		mp = dirInode
	}

	mp.mu.Lock()
	defer mp.mu.Unlock()

	prev := mp.findChildUnlocked(name)
	if prev == nil {
		mountInode := NewInode(fs, mp, &name)
		mountInode.ToDir()
		mountInode.dir.cloud = b.cloud
		mountInode.dir.mountPrefix = b.prefix
		mountInode.AttrTime = TIME_MAX

		fs.mu.Lock()
		defer fs.mu.Unlock()

		fs.insertInode(mp, mountInode)
		prev = mountInode
	} else {
		if !prev.isDir() {
			panic(fmt.Sprintf("inode %v is not a directory", *prev.FullName()))
		}

		// This inode might have some cached data from a parent mount.
		// Clear this cache by resetting the DirTime.
		// Note: resetDirTimeRec should be called without holding the lock.
		prev.resetDirTimeRec()
		prev.mu.Lock()
		defer prev.mu.Unlock()
		prev.dir.cloud = b.cloud
		prev.dir.mountPrefix = b.prefix
		prev.AttrTime = TIME_MAX

	}
	fuseLog.Infof("mounted /%v", *prev.FullName())
	b.mounted = true
}

func (fs *DatasetFS) MountAll(mounts []*Mount) {
	fs.mu.RLock()
	root := fs.getInodeOrDie(fuseops.RootInodeID)
	fs.mu.RUnlock()

	for _, m := range mounts {
		fs.mount(root, m)
	}
}



func (fs *DatasetFS) SigUsr1() {
	fs.mu.RLock()

	log.Infof("forgot %v inodes", fs.forgotCnt)
	log.Infof("%v inodes", len(fs.inodes))
	fs.mu.RUnlock()
	debug.FreeOSMemory()
}

// Find the given inode. Panic if it doesn't exist.
//
// RLOCKS_REQUIRED(fs.mu)
func (fs *DatasetFS) getInodeOrDie(id fuseops.InodeID) (inode *Inode) {
	inode = fs.inodes[id]
	if inode == nil {
		panic(fmt.Sprintf("Unknown inode: %v", id))
	}

	return
}

func (fs *DatasetFS) Mount(mount *Mount) {
	fs.mu.RLock()
	root := fs.getInodeOrDie(fuseops.RootInodeID)
	fs.mu.RUnlock()
	fs.mount(root, mount)
}

func (fs *DatasetFS) Unmount(mountPoint string) {
	fs.mu.RLock()
	mp := fs.getInodeOrDie(fuseops.RootInodeID)
	fs.mu.RUnlock()

	fuseLog.Infof("Attempting to unmount %v", mountPoint)
	path := strings.Split(strings.Trim(mountPoint, "/"), "/")
	for _, localName := range path {
		dirInode := mp.findChild(localName)
		if dirInode == nil || !dirInode.isDir() {
			fuseLog.Errorf("Failed to find directory:%v while unmounting %v. "+
				"Ignoring the unmount operation.", localName, mountPoint)
			return
		}
		mp = dirInode
	}
	mp.ResetForUnmount()
	return
}

func (fs *DatasetFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {

	// TODO: With a manifest, we can be more accurate than this

	const BLOCK_SIZE = 4096
	const TOTAL_SPACE = 1 * 1024 * 1024 * 1024 * 1024 * 1024 // 1PB
	const TOTAL_BLOCKS = TOTAL_SPACE / BLOCK_SIZE
	const INODES = 1 * 1000 * 1000 * 1000 // 1 billion
	op.BlockSize = BLOCK_SIZE
	op.Blocks = TOTAL_BLOCKS
	op.BlocksFree = TOTAL_BLOCKS
	op.BlocksAvailable = TOTAL_BLOCKS
	op.IoSize = 1 * 1024 * 1024 // 1MB
	op.Inodes = INODES
	op.InodesFree = INODES
	return
}

func (fs *DatasetFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	attr, err := inode.GetAttributes()
	if err == nil {
		op.Attributes = *attr
		//op.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
		op.AttributesExpiration = TIME_MAX  // immutable
	}

	return
}



func (fs *DatasetFS) allocateInodeId() (id fuseops.InodeID) {
	id = fs.nextInodeID
	fs.nextInodeID++
	return
}


// LookUpInode is implemented assuming that initialization is complete (i.e.
// the inode tree does not change)
func (fs *DatasetFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {

	var inode *Inode
	defer func() { fuseLog.Debugf("<-- LookUpInode %v %v %v", op.Parent, op.Name, err) }()

	// Get inode of Parent given ParentInodeId
	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	// Find the inode for the appropriate child.
	parent.mu.Lock()
	inode = parent.findChildUnlocked(op.Name)
	if inode != nil {
		inode.Ref()
	} else {
		return fuse.ENOENT
	}
	parent.mu.Unlock()


	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.TypeCacheTTL)

	return
}

// LOCKS_REQUIRED(fs.mu)
// LOCKS_REQUIRED(parent.mu)
func (fs *DatasetFS)  insertInode(parent *Inode, inode *Inode) {
	addInode := false
	if *inode.Name == "." {
		inode.Id = parent.Id
	} else if *inode.Name == ".." {
		inode.Id = fuseops.InodeID(fuseops.RootInodeID)
		if parent.Parent != nil {
			inode.Id = parent.Parent.Id
		}
	} else {
		if inode.Id != 0 {
			panic(fmt.Sprintf("inode id is set: %v %v", *inode.Name, inode.Id))
		}
		inode.Id = fs.allocateInodeId()
		addInode = true
	}
	parent.insertChildUnlocked(inode)
	if addInode {
		fs.inodes[inode.Id] = inode

		// if we are inserting a new directory, also create
		// the child . and ..
		if inode.isDir() {
			fs.addDotAndDotDot(inode)
		}
	}
}

func (fs *DatasetFS) addDotAndDotDot(dir *Inode) {
	dot := NewInode(fs, dir, PString("."))
	dot.ToDir()
	dot.AttrTime = TIME_MAX
	fs.insertInode(dir, dot)

	dot = NewInode(fs, dir, PString(".."))
	dot.ToDir()
	dot.AttrTime = TIME_MAX
	fs.insertInode(dir, dot)
}



func (fs *DatasetFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()

	handleID := fs.nextHandleID
	fs.nextHandleID++

	in := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	// XXX/is this a dir?
	dh := in.OpenDir()

	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.dirHandles[handleID] = dh
	op.Handle = handleID

	return
}



func (fs *DatasetFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {

	fs.mu.RLock()
	dh := fs.dirHandles[op.Handle]
	fs.mu.RUnlock()

	if dh == nil {
		panic(fmt.Sprintf("can't find dh=%v", op.Handle))
	}

	inode := dh.inode
	inode.logFuse("ReadDir", op.Offset)

	// I'm almost certain we don't need this locking now that DirHandle no longer checks against the cloud
	//dh.mu.Lock()
	//defer dh.mu.Unlock()

	for i := op.Offset; ; i++ {
		e := dh.ReadDir(i)
		if e == nil {
			break
		}

		if e.Inode == 0 {
			panic(fmt.Sprintf("unset inode %v", e.Name))
		}

		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], makeDirEntry(e))
		if n == 0 {
			break
		}

		dh.inode.logFuse("<-- ReadDir", e.Name, e.Offset)

		op.BytesRead += n
	}

	return
}

func (fs *DatasetFS) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) (err error) {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	dh := fs.dirHandles[op.Handle]
	dh.CloseDir()

	fuseLog.Debugln("ReleaseDirHandle", *dh.inode.FullName())

	delete(fs.dirHandles, op.Handle)

	return
}

func (fs *DatasetFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.RLock()
	in := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	fh, err := in.OpenFile(op.Metadata)
	if err != nil {
		return
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh

	op.Handle = handleID
	op.KeepPageCache = true

	return
}

func (fs *DatasetFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {

	fs.mu.RLock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.RUnlock()

	op.BytesRead, err = fh.ReadFile(op.Offset, op.Dst)

	return
}


func (fs *DatasetFS) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fh := fs.fileHandles[op.Handle]
	fh.Release()

	fuseLog.Debugln("ReleaseFileHandle", *fh.inode.FullName(), op.Handle, fh.inode.Id)

	delete(fs.fileHandles, op.Handle)

	// try to compact heap
	//fs.bufferPool.MaybeGC()
	return
}



func (fs *DatasetFS) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	attr, err := inode.GetAttributes()
	if err == nil {
		op.Attributes = *attr
		op.AttributesExpiration = TIME_MAX
	}
	return
}





// Below this are no-op operations

func (fs *DatasetFS) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) (err error) {

	// intentionally ignored, so that write()/sync()/write() works
	// see https://github.com/kahing/goofys/issues/154
	return
}


func (fs *DatasetFS) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {

	return
}

func (fs *DatasetFS) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {

	return
}













