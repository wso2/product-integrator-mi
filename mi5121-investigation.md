# Investigating WSO2 MI #5121 on Windows

[Issue #5121](https://github.com/wso2/product-integrator-mi/issues/5121)

I’m an undergraduate and this was my first time tracing MI’s automation shutdown code. I started by trying to reproduce the report on Windows with the MI 4.7.0 distribution and Java 21 that I already had. The original report used MI 4.6.0. I used two small CARs to isolate the behavior.

## Getting a baseline

I ran each CAR separately with `micro-integrator.bat --car <app-name>`. The first logged `MI5121_SUCCESS` and returned to PowerShell after a graceful shutdown. The second deliberately read a missing configuration value into a JSON variable. MI logged the evaluation error, ran the sequence configured with `onError`, and printed `MI5121_FAULT`. It stayed running until I pressed Ctrl+C.

Getting that comparison took a few attempts. An initial launch in the assisted shell hit a Java loopback permission error. Running MI in regular PowerShell got past startup. The first CAR also failed to extract because its ZIP lacked explicit directory entries. After repacking it, MI deployed the CAR but skipped the sequences because the generic filenames `Main.xml` and `Fault.xml` collided with files already in the test install. Giving each sequence a unique name cleared that problem. Those setup errors happened before either test sequence ran.

## Following the shutdown path

The successful run gave me a starting point. `MicroIntegratorSequenceController.complete()` requests graceful shutdown when the completed sequence matches the configured automation main sequence. In `ServiceBusInitializer`, MI sets that sequence name on the message context and calls Synapse’s `injectMessage(...)`.

The fault log narrowed the investigation. The handler had run, so I needed to understand what MI did when Synapse returned from it. In the path we reproduced, Synapse catches the mediation exception, invokes the fault handler, and returns `false`. MI discarded that result and relied on the main sequence’s completion callback for shutdown. My first patch checked the return value inside automation startup and requests graceful shutdown when it is `false`. The existing callback still handled normal completion.

## Building the patched runtime

The source clone downloaded successfully, but Windows refused to check out some long paths. We recovered the existing checkout with Git’s repository-local `core.longpaths` setting. I also had to set up Maven in PowerShell alongside Java 21 before I could build.

My first build, `mvn -pl distribution -am clean install -DskipTests`, ran for 27 minutes and failed while assembling the distribution ZIP. I had expected Maven to include everything packaging needed. The assembly was looking for files under `p2-profile/target`, and the selected modules hadn't generated them. I retried with `mvn -Pfull install -DskipTests`. The full build generated the profile and finished with `BUILD SUCCESS` after 36 minutes and 41 seconds.

## What I verified before the first push

I extracted the patched 4.7.0-SNAPSHOT distribution separately and reran the two CARs in regular PowerShell.

| Case | Baseline MI 4.7.0 | Patched MI 4.7.0-SNAPSHOT |
| --- | --- | --- |
| Normal completion | Logged `MI5121_SUCCESS` and shut down gracefully | Logged `MI5121_SUCCESS` and shut down gracefully |
| Main sequence fails and runs `onError` | Logged `MI5121_FAULT` and stayed up until Ctrl+C | Logged `MI5121_FAULT`, then `Micro Integrator Shutdown complete`, and returned to PowerShell automatically |

The patched Windows launcher reported exit code `0` for the fault run. The manual tests verify shutdown for these two cases. I didn’t verify other kinds of fault sequences, and the Maven build ran with automated tests skipped. Failure signaling through the process exit code is still open.

## After the first push

CodeRabbit raised a question I hadn’t tested. What happens if a sequence makes a nonblocking call and still has work to do when the current call returns? Both of my original CARs ran straight through. Passing those tests made me too confident about using the return value of `injectMessage(...)` to decide when to shut down.

I ran a local endpoint that waited about five seconds before replying. I put the call in two different places. With the delay in the main sequence, I saw `ASYNC_MAIN_RESUMED`, followed by the deliberate error and `ASYNC_MAIN_FAULT_DONE`. The handler had finished, but I had to press Ctrl+C. With the delay inside the fault handler, I got the opposite result. MI started shutting down 39 milliseconds after `ASYNC_FAULT_STARTED`. I never saw `ASYNC_FAULT_DONE`. Until then I had counted an automatic exit as a pass. In this case it meant part of the handler never ran.

### Moving the decision to sequence completion

I dropped the shutdown check in `ServiceBusInitializer` and tried making the decision in `MicroIntegratorSequenceController.complete()`. On completion, the observer reads the automation main sequence name from the current message context. It looks up that sequence’s configured error handler and requests shutdown when the completed sequence matches either the main sequence or that handler. This lets the delayed call resume and run the rest of the sequence before MI shuts down.

My first retest made this change look like it had fixed nothing. I had rebuilt MI and extracted the ZIP into a new folder, so I assumed I was testing the new code. The async main case still needed Ctrl+C. The async fault case still shut down early. Comparing the compiled classes finally explained why those runs were misleading. Both the controller and initializer class files in the extracted runtime differed from the ones in the module I had just built. I could verify the mismatch. I didn’t establish which packaging step caused it.

I backed up the initializer JAR in the stopped test runtime, copied in the one from the module’s `target` directory, and checked that the two JARs had the same SHA-256 hash. Then I ran all four CARs again.

| Case | What I saw with the verified JAR |
| --- | --- |
| Normal completion | `MI5121_SUCCESS`, then graceful shutdown. |
| Synchronous fault | `MI5121_FAULT`, then `Micro Integrator Shutdown complete`. This was the original case that needed Ctrl+C. |
| Delay in the main sequence, then a fault | `ASYNC_MAIN_RESUMED`, then `ASYNC_MAIN_FAULT_DONE`, then graceful shutdown. |
| Delay inside the fault handler | About five seconds between `ASYNC_FAULT_STARTED` and `ASYNC_FAULT_DONE`. Shutdown began after the second marker. |

All four runs returned to PowerShell on their own. These are manual results for the locally replaced JAR. I haven’t verified a freshly packaged ZIP containing those same bytes. At the time of these notes the observer change was staged locally. The build also left four untracked `.mar` files under the integration-test resources, and I left those out of the staged change.

## Finding the remaining edge cases

The first change handled the cases I could reproduce, but I knew that did not cover every way a sequence could finish. CodeRabbit pointed out two cases that looked possible: a fault sequence could contain `<drop/>`, and a normal sequence call could refer to the same sequence that was also configured as the automation fault handler.

I added small unit tests for these cases. I already had a basic sequence test class, so I used it and ran the module with Java 21. The first test run reported seven tests, with only the resumed nested `<drop/>` test failing. It told me that an existing nested call could resume and end before the shutdown decision recognized it.

I then tried a continuation-state check. That fix was not enough, and a later test showed a worse problem: an ordinary sequence call could appear terminal and shut down before the main sequence continued. I had to stop and change the approach rather than treating the test failure as just a timing issue.

### Keeping nested sequences non-terminal

I changed the sequence wrapper so there were two kinds of wrapped sequences: terminal sequences for the automation main and fault flows, and non-terminal sequences for ordinary nested sequence references. When a nested sequence contained `<drop/>`, I replaced that mediator with a wrapper that requested shutdown only if the wrapper reached the drop.

This was not a particularly elegant implementation at first. I was working through Synapse behavior one case at a time and kept checking whether the change stopped ordinary sequence calls too early. The unit test for normal sequence use was useful here: it should end with the shared sequence, the rest of the main sequence, and then shutdown in that order.

After this change, I ran all seven focused tests again. The result was:

```text
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

The test named `shutsDownAfterResumedNestedSequenceDrops` passed. That was the case that had first exposed the remaining problem.

I later checked two more edge cases raised during review. One was a cycle in `onError` chains, where a fault sequence could point back to a sequence that was already being wrapped. I reused private sequence names and tracked sequences already being wrapped so the construction stopped without an infinite recursive call.

The second case was a nested sequence reference that followed another mediator and resumed asynchronously. I preserved the original mediator position on the private sequence reference and added a test that expected the next main-sequence mediator to run after the nested sequence resumed.

I also removed the continuation check that treated `!hasChild()` as proof that a suspended flow had finished. Shutdown now depends on normal terminal-flow completion or an explicit drop wrapper. This avoids treating a suspended continuation as terminal merely because it has no child state.

The focused suite was expanded to eleven tests. It includes branch-mediated nested sequence fault handling, as well as the wrapper’s content-aware message-building path. The full result was:

```text
Tests run: 11, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## Building and testing the complete distribution

The focused module tests were useful, but I still wanted to test a real distribution. I installed JDK 21 and ran the complete Maven reactor with tests skipped. The build took about 24 minutes and finished with `BUILD SUCCESS`. It produced:

```text
distribution/target/wso2mi-4.7.0-SNAPSHOT.zip
```

I extracted that ZIP into a separate runtime folder so I could test the packaged classes without relying on another module build.

The first runtime attempts were not valid tests. I had to learn the CApp archive layout. A ZIP made by PowerShell did not include explicit directory entries and MI could not extract the nested `main` and `fault` folders. I rebuilt the CApp with explicit directory entries using Python's ZIP support.

I also made mistakes with the launcher arguments. The command with `run -car mi5121Automation` put `-car` after the lifecycle action, and the Java command showed an empty `automation.mode.seq.car.name`. Starting it with `-car mi5121Automation` was the launcher form that set the CApp name correctly.

Once the CApp was packed and launched correctly, its two sequences deployed successfully. The main sequence logged `MAIN_STARTED`, threw an explicit test error, and ran the fault sequence, which logged `FAULT_STARTED`. MI then started graceful shutdown and printed `Micro Integrator Shutdown complete`. The Java process exited on its own.

This was the complete-distribution check I had been missing earlier. It did not replace the focused tests, but it confirmed that the sequence wrapper was present in a packaged runtime and that the callback could trigger shutdown through the real startup path.

## What I would do differently

- I spent several attempts getting a usable reproduction. In hindsight, I should have checked the CAR layout and confirmed that both sequences deployed before using a run to draw conclusions about shutdown. Once `MI5121_FAULT` actually appeared, I could move past deployment and focus on what MI does after `injectMessage(...)` returns.
- I started the clone without accounting for Windows path limits. The download finished, but the checkout was incomplete. Next time I’d enable long-path support for the clone and check `git status` before creating the branch.
- I launched the first build without checking where the assembly descriptor got its inputs. Checking the reference to `p2-profile/target` against the selected reactor modules would have caught the missing step before the 27-minute build.
- I checked the fault exit code only after the shutdown test. Next time I’d record the launcher’s exit code immediately for both runs. Seeing the PowerShell prompt return proves the process ended. Checking the exit code tells me what the launcher reported to its caller.

I ran the Windows builds and reproduction checks and captured the results. I used AI to help me understand how MI and Synapse handle sequence completion in automation mode. In particular, it helped me break down how MI uses a Synapse sequence observer to trigger graceful shutdown and how that callback differs from the return value of `injectMessage(...)`.

## Follow-up regressions after review

I reproduced four additional shutdown cases on Windows and retested them on the updated distribution.

| Case | What I reproduced | What I verified after the change |
| --- | --- | --- |
| Shared sequence invoked while automation waits | An unrelated API request reached a drop and started shutdown before the automation response was released. | The API request reached the drop while automation remained suspended. Shutdown began after automation resumed and finished. |
| Drop consumed by ForEach | The loop returned control to the outer sequence, but shutdown began while its delayed HTTP call was pending. | The outer sequence waited for the five-second response and logged its final marker before shutdown. |
| Expression-selected child sequence | The child fault handler finished, but MI stayed running until I pressed Ctrl+C. | MI began graceful shutdown after the child fault handler finished and exited on its own. |
| Terminal drop in a switch case | The matching case reached the drop and ended mediation, but MI stayed running until I pressed Ctrl+C. | The pre-drop marker appeared, the post-switch marker stayed absent, and MI completed graceful shutdown on its own. |

I scoped drop-triggered shutdown to the owning automation invocation, kept the original ForEach iteration behavior, and resolved expression-selected sequences at mediation time. A focused two-invocation test also reproduced mutation of the deployed filter's children. Automation now creates private filter and switch branch containers before wrapping their children. The controller tests check matching and default switch drops, normal switch continuation, and preservation of the deployed branch children.

All 15 tests in `MicroIntegratorSequenceControllerTest` passed with zero failures, errors, or skips. On the packaged Windows runtime, all nine launcher retests exited with code `0` and logged their expected markers before shutdown began. These were normal completion, synchronous fault, async main fault, async fault handler, terminal drop, ordinary sequence call, shared A/B, delayed ForEach, and dynamic fault. The separate switch CAR also completed graceful shutdown and exited on its own. The tested initializer JAR matched the module build by SHA-256:

`F9C47A212EF485F4201433A7131FA9DE8254D7AFA3516B47E1C7EA808A58DC7B`

These results cover the mediator paths exercised by the fixtures.
