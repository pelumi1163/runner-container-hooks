/* eslint-disable @typescript-eslint/no-unused-vars */
import * as fs from 'fs'
import * as core from '@actions/core'
import { RunScriptStepArgs } from 'hooklib'
import { execDetachedScriptStep } from '../k8s'
import {
  formatError,
  writeEntryPointScript,
  writeDetachedLauncherScript
} from '../k8s/utils'
import { JOB_CONTAINER_NAME } from './constants'

export async function runScriptStep(
  args: RunScriptStepArgs,
  state,
  responseFile
): Promise<void> {
  const { entryPoint, entryPointArgs, environmentVariables } = args
  const { containerPath, runnerPath } = writeEntryPointScript(
    args.workingDirectory,
    entryPoint,
    entryPointArgs,
    args.prependPath,
    environmentVariables
  )
  const launcher = writeDetachedLauncherScript(containerPath)

  try {
    await execDetachedScriptStep(
      launcher.containerPath,
      state.jobPod,
      JOB_CONTAINER_NAME
    )
  } catch (err) {
    const message = formatError(err)
    core.debug(`execDetachedScriptStep failed: ${message}`)
    throw new Error(`failed to run script step: ${message}`)
  } finally {
    fs.rmSync(runnerPath)
    fs.rmSync(launcher.runnerPath)
  }
}
