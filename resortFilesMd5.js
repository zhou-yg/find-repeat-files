const fsP = require('node:fs/promises')
const fs = require('node:fs')
const path = require('node:path')
const crypto = require('crypto')

const readline = require('node:readline/promises');
const { stdin, stdout } = require('node:process');

const rl = readline.createInterface({ input: stdin, output: stdout });

const duplicateDirName = 'the_duplicates'
const exportDirName = 'the_exports'
const md5CacheName = 'the_md5Cache.json'

// const entryDir = path.join(__dirname, './test')
const entryDir = '/Volumes/Seagate Basic/所有照片'

const exportDir = path.join(entryDir, exportDirName)
const duplicateDir = path.join(entryDir, duplicateDirName)

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

  st = Date.now()

  await recordFiles(allFiles)

  console.log(`end allFiles, cost ${ (Date.now() - st)/1000 } s`)
  st = Date.now()

  await recordFiles(exportAllFiles)

  console.log(`end exportAllFiles, cost ${ (Date.now() - st)/1000 } s`)
})


async function recordFiles (files) {
  let fi = 0

  let T_MAX = 1000

  let queue = []

  const store = new MD5Store()

  for (const fo of files) {
    if (queue.length < T_MAX) {
      queue.push(async () => {
        if (store.get(fo.path)) {
          return
        }

        let cur = fi++
        const cst = Date.now()
        const md5 = await getMD5(fo.path)
        console.log(`${cur}, ${fo.path} ${md5}, cost ${ (Date.now() - cst)/1000 } s`)
        fo.md5 = md5
        store.set(fo.path, md5)
      })
    } else {
      await Promise.all(queue.map(f => f()))
      queue = []
    }
  }
  await Promise.all(queue.map(f => f()))
}


class MD5Store {
  cacheFile = path.join(entryDir, md5CacheName)

  get(k) {
    if (fs.existsSync(this.cacheFile)) {
      const cache = fs.readFileSync(this.cacheFile, 'utf-8')
      const json = JSON.parse(cache)
      return json[k]
    }
  }

  set (k, v) {
    if (fs.existsSync(this.cacheFile)) {
      const cache = fs.readFileSync(this.cacheFile, 'utf-8')
      const json = JSON.parse(cache)
      json[k] = v
      fs.writeFileSync(this.cacheFile, JSON.stringify(json, null, 2))
    } else {
      fs.writeFileSync(this.cacheFile, JSON.stringify({[k]: v}, null, 2))
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