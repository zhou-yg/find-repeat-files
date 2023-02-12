const fsP = require('node:fs/promises')
const fs = require('node:fs')
const path = require('node:path')
const crypto = require('crypto')

const readline = require('node:readline/promises');
const { stdin, stdout } = require('node:process');

const rl = readline.createInterface({ input: stdin, output: stdout });

const duplicateDirName = 'the_duplicates'
const exportDirName = 'the_exports'

// const entryDir = path.join(__dirname, './test')
const entryDir = '/Volumes/Seagate Basic/所有照片'

const exportDir = path.join(entryDir, exportDirName)
const duplicateDir = path.join(entryDir, duplicateDirName)

if (!fs.existsSync(duplicateDir)) {
  fs.mkdirSync(duplicateDir)
}
if (!fs.existsSync(exportDir)) {
  fs.mkdirSync(exportDir)
}

let st = Date.now()
const allFiles = []
let si = 1

const exportAllFiles = []

traverse(entryDir, allFiles).then(async (r) => {
  console.log(`allFiles: ${allFiles.length}, cost ${ (Date.now() - st)/1000 } s`)

  st = Date.now()
  si = 1

  const exportFiles = await traverse(exportDir, exportAllFiles)

  console.log(`exportAllFiles: ${exportFiles.length}, cost ${ (Date.now() - st)/1000 } s`)

  const answer = 'yes' // await rl.question('do calculate right now ? (yes/no) : ');
  if (answer === 'yes') {

    const vfs = new VirtualFS(exportAllFiles)

    st = Date.now()

    let fi = 0

    let T_MAX = 1000

    let queue = []

    for (const file of allFiles) {
      if (queue.length < T_MAX) {
        queue.push(async () => {
          let cur = fi++
          const cst = Date.now()
          console.log(`${cur}. move file: `, file.path);
          await vfs.moveTo(
            path.join(exportDir, file.birthTimeStr),
            file
          )    
          console.log(`${cur}. move file end: `, file.path, `${(Date.now() - cst) / 1000}s`);
        })
      } else {
        await Promise.all(queue.map(fn => fn()))
        console.log(`${fi}.vfs info: created ${vfs.createdFiles.length}, duplicates ${vfs.duplicates.length}, cost ${ (Date.now() - st)/1000 } s`)
        console.log(`${fi}.vfs info2: duplicates ${vfs.duplicatesSize()}, cost ${ (Date.now() - st)/1000 } s`)
    
        queue = []
      }
    }
    await Promise.all(queue.map(fn => fn()))

    console.log(`vfs info: created ${vfs.createdFiles.length}, duplicates ${vfs.duplicates.length}, cost ${ (Date.now() - st)/1000 } s`)
    console.log(`vfs info2: duplicates ${vfs.duplicatesSize()}, cost ${ (Date.now() - st)/1000 } s`)

    st = Date.now()
    const answer2 = await rl.question('do write right now ? (yes/no) : ')
    rl.close()

    if (answer2 === 'yes') {

    }
  }
})

class VirtualFS {
  duplicates = []
  createdFiles = []
  constructor (allFiles) {
    this.allFiles = allFiles.slice()
  }

  duplicatesSize () {
    return this.duplicates.reduce((p, n) => {
      return p + n.sizeMB
    }, 0)
  }

  mkdir (path) {
    const fo = new FileObj(path, true)
    this.createdFiles.push(fo)
  }

  exist (path) {
    return !!this.virtualAllFiles().find(fo => fo.path === path)
  }

  virtualAllFiles () {
    return this.createdFiles.concat(this.allFiles)
  }

  // 直接的children
  getChildrenFiles (dir) {
    const dirPathArr = dir.split('/').filter(Boolean)
    return this.virtualAllFiles().filter(fo => {
      return (
        !fo.dir && 
        fo.pathArr.slice(0, dirPathArr.length).join('') === dirPathArr.join('')
      )
    })
  }
  // createDirWithChildren (dir, fileObj) {
  //   const fo = new FileObj(dir, true)
  //   this.createdFiles.push(fo)


  //   const dfs = (fo) => {
  //     fo.children.forEach(cfo => {
  //       const relativePath = path.relative(fileObj.path, cfo.path)
  //       const cfoNewPath = path.join(dir, relativePath)
  //       const newCfo = new FileObj(cfoNewPath, cfo.dir)
  //       this.createdFiles.push(newCfo)
  //       dfs(cfo)
  //     })
  //   }
  //   dfs(fileObj)
  // }

  // // merge fileObj/* to destDir/*
  // async mergeChildren (destDir, fileObj) {
  //   for (const cfo of fileObj.children) {
  //     console.log('cfo: ', destDir, cfo.path);
  //     if (cfo.dir) {
  //       const subDestDir = path.join(destDir, cfo.pathArr[cfo.pathArr.length - 1])
  //       console.log('subDestDir: ', subDestDir);
  //       if (this.exist(subDestDir)) {
  //         await this.mergeChildren(subDestDir, cfo)
  //       } else {
  //         this.createDirWithChildren(subDestDir, cfo)
  //       }
  //     } else {
  //       await this.moveTo(destDir, cfo)
  //     }
  //   }
  // }

  // async mergeDir(fileObj) {
  //   if (fileObj.dir) {
  //     const destDir = path.join(exportDir, fileObj.birthTimeStr)
  //     if (this.exist(destDir)) { 
  //       await this.mergeChildren(destDir, fileObj)
  //     } else {
  //       this.createDirWithChildren(destDir, fileObj)
  //     }
  //   }
  // }

  async moveTo (destDir, fileObj) {

    await fileObj.getMD5Async() 
    
    let children, syncMode;
    
    do {
      children = this.getChildrenFiles(destDir)
      syncMode = children.every(fo => fo.md5)
      if (!syncMode) {
        await Promise.all(children.map(fo => fo.getMD5Async()))
      }
    } while (!syncMode)

    const existFile = children.find(fo => fo.md5 === fileObj.md5)

    const newFileObj = new FileObj(
      path.join(destDir, fileObj.relativePath),
      fileObj
    )
    if (syncMode) {
      if (existFile) {
        this.duplicates.push(newFileObj)
      } else {
        this.createdFiles.push(newFileObj)
      }
    }
  }
}


class FileObj {
  path = ''
  pathArr = []

  md5 = null
  md5Promise = null
  birthTimeStr = ''  
  sourceFilePath = null
  sizeMB = 0

  constructor (fileOrDir, sourceFileObj) {
    this.path = fileOrDir
    this.pathArr = fileOrDir.split('/').filter(Boolean)

    if (sourceFileObj) {
      ['sizeMB', 'md5', 'birthTimeStr'].forEach(k => {
        this[k] = sourceFileObj[k]
      })
      this.sourceFilePath = sourceFileObj.path
    }
  }

  readablePath () {
    return this.sourceFilePath || this.path
  }

  getMD5Async () {
    if (this.md5) {
      return this.md5
    }
    if (!this.md5Promise) {
      this.md5Promise = getMD5(this.readablePath())
      this.md5Promise.then(md5 => {
        this.md5 = md5
      })
    }
    return this.md5Promise
  }

  init (stat) {
    this.sizeMB = stat.size / 1000 / 1000

    this.pathArr.some((pn, i) => {
      if (isDateDir(pn)) {
        this.birthTimeStr = pn
        this.relativePath = this.pathArr.slice(i + 1).join('/')
        return true
      }
    })
    if (!this.birthTimeStr) {
      const { birthtime } = stat
      this.birthTimeStr = `${birthtime.getFullYear()}年${birthtime.getMonth() + 1}月${birthtime.getDate()}日`
      this.relativePath = `${last(this.pathArr)}`
    }
  }
}


async function traverse (dir, allFiles) {
  if (!validate(dir)) {
    return []
  }

  const files = await fsP.readdir(dir)

  const children = await Promise.all(files.map(async (file) => {
    const fileOrDir = path.join(dir, file)
    if (validate(fileOrDir)) {      
      console.log(`${si++} scan file: ${fileOrDir}`, )
  
      const fileObj = new FileObj(fileOrDir)
      const stat = await fsP.stat(fileOrDir)
      fileObj.init(stat)

      if (stat.isDirectory()) {
        await traverse(fileOrDir, allFiles)
      } else {
        allFiles.push(fileObj)
      }
      return fileObj
    }
  }))

  return children.filter(Boolean)
}

function validate(dir) {
  return !/the_/.test(dir)
}

function isDateDir(dir) {
  return /^\d+年\d+月\d+日$/.test(dir)
}

function getMD5 (file) {
  return new Promise(resolve => {
    const md5 = crypto.createHash('md5')
    const readStream = fs.createReadStream(file)
    readStream.on('data', (chunk) => {
      md5.update(chunk)
    })
    readStream.on('end', () => {
      const str = md5.digest('hex')
      resolve(str)
    })
  })
}

function last(arr) {
  return arr[arr.length - 1]
}