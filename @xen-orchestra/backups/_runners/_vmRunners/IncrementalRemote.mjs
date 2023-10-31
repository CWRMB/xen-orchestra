import { asyncEach } from '@vates/async-each'
import { decorateMethodsWith } from '@vates/decorate-with'
import { defer } from 'golike-defer'
import { Task } from '@vates/task'
import assert from 'node:assert'
import isVhdDifferencingDisk from 'vhd-lib/isVhdDifferencingDisk.js'
import mapValues from 'lodash/mapValues.js'

import { AbstractRemote } from './_AbstractRemote.mjs'
import { forkDeltaExport } from './_forkDeltaExport.mjs'
import { IncrementalRemoteWriter } from '../_writers/IncrementalRemoteWriter.mjs'

class IncrementalRemoteVmBackupRunner extends AbstractRemote {
  _getRemoteWriter() {
    return IncrementalRemoteWriter
  }
  async _run($defer) {
    const transferList = await this._computeTransferList(({ mode }) => mode === 'delta')
    await this._callWriters(async writer => {
      await writer.beforeBackup()
      $defer(async () => {
        await writer.afterBackup()
      })
    }, 'writer.beforeBackup()')

    if (transferList.length > 0) {
      for (const metadata of transferList) {
        assert.strictEqual(metadata.mode, 'delta')

        await this._callWriters(writer => writer.prepare({ isBase: metadata.isBase }), 'writer.prepare()')
        const incrementalExport = await this._sourceRemoteAdapter.readIncrementalVmBackup(metadata, undefined, {
          useChain: false,
        })

        const differentialVhds = {}

        await asyncEach(Object.entries(incrementalExport.streams), async ([key, stream]) => {
          differentialVhds[key] = await isVhdDifferencingDisk(stream)
        })

        incrementalExport.streams = mapValues(incrementalExport.streams, this._throttleStream)
        await this._callWriters(
          writer =>
            writer.transfer({
              deltaExport: forkDeltaExport(incrementalExport),
              differentialVhds,
              timestamp: metadata.timestamp,
              vm: metadata.vm,
              vmSnapshot: metadata.vmSnapshot,
            }),
          'writer.transfer()'
        )
        await this._callWriters(writer => writer.cleanup(), 'writer.cleanup()')
        // for healthcheck
        this._tags = metadata.vm.tags
      }
    } else {
      Task.info('No new data to upload for this VM')
    }
  }
}

export const IncrementalRemote = IncrementalRemoteVmBackupRunner
decorateMethodsWith(IncrementalRemoteVmBackupRunner, {
  _run: defer,
})
