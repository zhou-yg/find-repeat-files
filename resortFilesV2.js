const fsP = require('node:fs/promises')
const fs = require('node:fs')
const path = require('node:path')
const crypto = require('crypto')

const readline = require('node:readline/promises');
const { stdin, stdout } = require('node:process');

const rl = readline.createInterface({ input: stdin, output: stdout });

const duplicateDirName = 'the_duplicates2'
const exportDirName = 'the_exports'
const md5CacheName = 'the_md5Cache.json'

// const entryDir = path.join(__dirname, './test')
const entryDir = '/Volumes/Seagate Basic/所有照片'

const exportDir = path.join(entryDir, exportDirName)
const duplicateDir = path.join(entryDir, duplicateDirName)
const md5CacheJSON = path.join(entryDir, md5CacheName)

const md5CacheObj = JSON.parse(fs.readFileSync(md5CacheJSON, 'utf-8').toString())

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


const strangeFiles = [
  '/Volumes/Seagate Basic/所有照片/电脑-照片导出/IMG_1070 (1).jpg',
  '/Volumes/Seagate Basic/所有照片/电脑-照片导出/IMG_2796.jpg',
  '/Volumes/Seagate Basic/所有照片/电脑-照片导出/IMG_5382.jpg',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_0025 (1).JPG',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_1098.JPG',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_2587.HEIC',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_3817.HEIC',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_5611.mov',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_7511 (1).mov',
  '/Volumes/Seagate Basic/所有照片/2021.02.28/岸上蓝山星河苑 - 杭州市, 浙江省, 2020年11月24日/IMG_3513.HEIC',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/2019年3月20日/IMG_3701.PNG',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/岸上蓝山星河苑 - 杭州市, 浙江省, 2018年11月29日/IMG_2951.JPG',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/岸上蓝山星河苑 - 杭州市, 浙江省, 2019年3月21日/IMG_3710.MOV',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/岸上蓝山星河苑 - 杭州市, 浙江省, 2020年7月3日/IMG_1088.JPG',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/西溪 - 杭州市, 浙江省, 2016年2月15日/IMG_1096 (1).JPG',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/闲林 - 杭州市, 浙江省, 2019年6月5日/IMG_4686.HEIC',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/2016年5月13日/IMG_1153 (1).PNG',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/2019年9月2日/IMG_4414.PNG',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/古荡 - 杭州市, 浙江省, 2019年8月21日/IMG_4379.JPG',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/岸上蓝山星河苑 - 杭州市, 浙江省, 2018年12月9日/IMG_3034.HEIC',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/岸上蓝山星河苑 - 杭州市, 浙江省, 2020年3月4日/IMG_5145.mov',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/杭州市 - 桐庐县 - 浙江省, 2016年6月25日/IMG_1234.JPG',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/绍兴市 - 越城区 - 浙江省, 2019年5月3日/IMG_4019.mov',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/闲林 - 杭州市, 浙江省, 2019年9月6日/IMG_4421.mov'
];
[
  '/Volumes/Seagate Basic/所有照片/电脑-照片导出/IMG_1070 (1).jpg',
  '/Volumes/Seagate Basic/所有照片/电脑-照片导出/IMG_2799.jpg',
  '/Volumes/Seagate Basic/所有照片/电脑-照片导出/IMG_5379.jpg',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_0024.JPG',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_1099 (1).JPG',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_2583.HEIC',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_3817.mov',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_5611.HEIC',
  '/Volumes/Seagate Basic/所有照片/电脑-照片原件/IMG_7511 (1).HEIC',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/2016年10月9日/IMG_1630.PNG',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/上海市 - 浦东新区 - 中国, 2020年8月8日/IMG_2039.mov',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/岸上蓝山星河苑 - 杭州市, 浙江省, 2018年3月31日/IMG_1024.HEIC',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/岸上蓝山星河苑 - 杭州市, 浙江省, 2019年8月21日/IMG_5774.HEIC',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/杭州市 - 余杭区 - 浙江省, 2020年5月23日/IMG_0058.mov',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/金华市 - 兰溪市 - 丹华路, 2019年8月15日/IMG_5709.MOV',
  '/Volumes/Seagate Basic/所有照片/2020.11.1照片/闲林 - 杭州市, 浙江省, 2020年5月10日/IMG_9877.mov',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/2016年5月20日/IMG_1158.JPG',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/2019年9月2日/IMG_4416.AAE',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/岸上蓝山星河苑 - 杭州市, 浙江省, 2018年10月11日/IMG_2516.mov',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/岸上蓝山星河苑 - 杭州市, 浙江省, 2018年12月9日/IMG_3028.mov',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/岸上蓝山星河苑 - 杭州市, 浙江省, 2020年2月8日/IMG_5019.JPG',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/杭州市 - 桐庐县 - 浙江省, 2016年6月25日/IMG_1237.JPG',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/绍兴市 - 越城区 - 浙江省, 2019年5月3日/IMG_4016.HEIC',
  '/Volumes/Seagate Basic/所有照片/zyg.mac.13.2021.03.06/Places/闲林 - 杭州市, 浙江省, 2020年10月1日/IMG_5704.HEIC'
];

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
      queue.push(async () => {
        let cur = fi++
        const cst = Date.now()
        await vfs.moveTo(
          path.join(exportDir, file.birthTimeStr),
          path.join(duplicateDir, file.birthTimeStr),
          file
        )    
        console.log(`${cur}. move file end: `, file.path, `${(Date.now() - cst) / 1000}s`);
      })

      if (queue.length >= T_MAX) {
        await Promise.all(queue.map(fn => fn()))
        console.log(`${fi}.vfs info: created ${vfs.createdFiles.length}, duplicates ${vfs.duplicates.length}, cost ${ (Date.now() - st)/1000 } s`)
        console.log(`${fi}.vfs info2: duplicates ${vfs.duplicatesSize()}, cost ${ (Date.now() - st)/1000 } s`)
    
        queue = []
      }
    }
    await Promise.all(queue.map(fn => fn()))

    console.log(`vfs info: created ${vfs.createdFiles.length}, duplicates ${vfs.duplicates.length}, cost ${ (Date.now() - st)/1000 } s`)
    console.log(`vfs info2: duplicates ${vfs.duplicatesSize()}, cost ${ (Date.now() - st)/1000 } s`)

    doubleCheck(allFiles, vfs)

    st = Date.now()
    const answer2 = await rl.question('do write right now ? (yes/no) : ')
    rl.close()

    if (answer2 === 'yes') {
      vfs.sort()

      writeFiles(vfs.createdFiles)
      
      writeFiles(vfs.duplicates)
    }
  }
})

// r 8317 - 2

let writeLimit = 10;
const dirExistMap = {}
function writeFiles (files) {

  for (let i = 0; i < files.length; i++) {
    // if (i >= writeLimit) {
    //   return
    // }

    const file = files[i]
    const source = file.sourceFilePath
    const target = file.path

    // make sure file paths
    tryMkdir(target)

    const newUniqueTargetPath = tryCopy(target)

    fs.renameSync(source, newUniqueTargetPath)

    console.log(`rename file end: ${source} -> ${newUniqueTargetPath}`)
  }
}

function tryCopy(targetPath) {
  const { dir, name, ext, base } = path.parse(targetPath)

  let i = 0
  let destFilePath = targetPath
  while (fs.existsSync(destFilePath)) {
    i++
    destFilePath = path.join(dir, `${name}(${i})${ext}`)
  }  
  return destFilePath
}

function tryMkdir(filePath) {
  const pathArr = filePath.split('/')
  pathArr.pop()

  let pathPre = ''
  pathArr.forEach(p => {
    pathPre = pathPre + '/' + p
    if (dirExistMap[pathPre]) {
      return
    }
    if (!fs.existsSync(pathPre)) {
      fs.mkdirSync(pathPre)
    }
    dirExistMap[pathPre] = true
  })
}

function doubleCheck (allFiles, vfs) {
  /**
   * make sure all files exist in vfs
   */
  let miss = 0
  const missFiles = []
  for (const file of allFiles) {
    if (file.md5 && vfs.duplicates.find(dfo => dfo.md5 === file.md5)) {
      continue
    }
    if (file.md5 && vfs.createdFiles.find(dfo => dfo.md5 === file.md5)) {
      continue
    }
    miss++
    missFiles.push(file)
  }
  console.log(`miss: ${miss}`)
  console.log('miss files:', missFiles.map(f => f.path))

  /**
   * make sure all vfs files exist in allFiles
   */
  let miss2 = 0 
  const missFiles2 = []
  for (const file of vfs.createdFiles) {
    if (file.md5 && allFiles.find(dfo => dfo.md5 === file.md5)) {
      continue
    }
    miss2++
    missFiles2.push(file)
  }
  console.log(`miss2: ${miss2}`)
  console.log('miss files2:', missFiles2.map(f => f.path))

  let miss3 = 0
  const missFiles3 = []
  for (const file of vfs.duplicates) {
    if (file.md5 && allFiles.find(dfo => dfo.md5 === file.md5)) {
      continue
    }
    miss3++
    missFiles3.push(file)
  }
  console.log(`miss3: ${miss3}`)
  console.log('miss files3:', missFiles3.map(f => f.path))
}


class VirtualFS {
  duplicates = []
  createdFiles = []
  constructor (allFiles) {
    this.allFiles = allFiles.slice()
  }

  sort () {
    this.duplicates.sort((a, b) => {
      return a.birthTimeNum() - b.birthTimeNum()
    })
    this.createdFiles.sort((a, b) => {
      return a.birthTimeNum() - b.birthTimeNum()
    })
  }

  duplicatesSize () {
    return this.duplicates.reduce((p, n) => {
      return p + n.sizeMB
    }, 0)
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

  async moveTo (destDir, destDir2, fileObj) {

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

    if (syncMode) {
      const newFileObj = new FileObj(
        path.join(
          existFile ? destDir2 : destDir,
          fileObj.relativePath
        ),
        fileObj
      )
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
        this.md5Promise = null
      })
    }
    return this.md5Promise
  }

  birthTimeNum () {
    const r = this.birthTimeStr.match(/\d+/)
    if (!r) {
      throw new Error(`birth time num error: ${this.birthTimeStr}`)
    }
    return Number(r.join(''))
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
    if (md5CacheObj[file]) {
      return resolve(md5CacheObj[file])
    }

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