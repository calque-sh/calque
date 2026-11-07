# calque-netshape

`calque-netshape` shapes loopback TCP on the ports `pnpm bench -- --network
shaped:<Mbit/s>/<RTT ms>` gives it. It runs as root through one sudoers rule,
so it is installed by hand, and no script ever installs it.

Everything it changes lives in kernel memory and is gone after a reboot: one
pf anchor, `com.apple/calque-netshape`, two dummynet pipes, 55601 and 55602,
and one pf enable reference. The script's header says how it shapes, and why
its scrub rules write a port as `N-1 >< N+1`.

## Install

Do every step yourself, in a Terminal no automated tool is driving, from a
checkout of a commit you have reviewed. The first step is the only one that
reads a path any process running as you can write, and nothing it copies is
trusted until you have read the root-owned copy.

1. Copy the script to a root-owned staging file that cannot run yet:

   ```sh
   sudo install -o root -g wheel -m 0444 scripts/netshape/calque-netshape /usr/local/libexec/calque-netshape.new
   ```

2. Read that installed copy in full, and compare its digest:

   ```sh
   less /usr/local/libexec/calque-netshape.new
   shasum -a 256 /usr/local/libexec/calque-netshape.new
   ```

   The reviewed script's SHA-256 is
   `55166240f35769ca4a1fd61f623865ec2f7535205aba671a731965df9e6d7f97`. The
   digest says the copy is the file this page describes; reading the copy is
   the check.

3. Make it executable and move it into place:

   ```sh
   sudo chmod 0755 /usr/local/libexec/calque-netshape.new
   sudo mv /usr/local/libexec/calque-netshape.new /usr/local/libexec/calque-netshape
   ```

4. Type the sudoers rule into a `.new` file. sudo's `#includedir` skips any
   name that contains a `.`, so the rule does nothing until step 6:

   ```sh
   sudo visudo -f /etc/sudoers.d/calque-netshape.new
   ```

   Type this one line, with your login name for `<user>`, and nothing else:

   ```text
   <user> ALL=(root) NOPASSWD: /usr/local/libexec/calque-netshape up *, /usr/local/libexec/calque-netshape down, /usr/local/libexec/calque-netshape status
   ```

   `up *` passes `up`'s arguments to the script, which checks them; `down`
   and `status` take none. sudo refuses any other use without a password.

5. Give it the owner and mode sudo requires, and check it:

   ```sh
   sudo chown root:wheel /etc/sudoers.d/calque-netshape.new
   sudo chmod 0440 /etc/sudoers.d/calque-netshape.new
   sudo visudo -cf /etc/sudoers.d/calque-netshape.new
   ```

6. Move it into place, and confirm that nothing is shaped:

   ```sh
   sudo mv /etc/sudoers.d/calque-netshape.new /etc/sudoers.d/calque-netshape
   sudo -n /usr/local/libexec/calque-netshape status
   ```

   `status` prints `enable token: none` and nothing else.

A changed script goes in through steps 1 to 3 again. The staging copy keeps
an unread script from running under the rule already in place. To remove
both: `sudo rm /etc/sudoers.d/calque-netshape /usr/local/libexec/calque-netshape`.

## In a run

`pnpm bench -- --network shaped:500/20` refuses before it starts unless the
installed script's SHA-256 equals this checkout's `calque-netshape`, sudo's
rule answers, and `status` reads `enable token: none`. It binds the listeners
the browser connects to, the edge's two or, under `--no-edge`, the page
server and `calque dev`, to ports in 45600–45699, calls `up` before the
reference pass, and calls `down` on every exit it can see, SIGINT and SIGTERM
included. A run killed with SIGKILL cannot: run
`sudo -n /usr/local/libexec/calque-netshape down` yourself, then `status`.

A shaped run is a second reading and is never judged, as a `--diagnostic`
run is not: its id ends in `-shaped`, its `passes` is false whatever its rows
say, and its label is directional.

## Calibration

`probe.py calibrate` times transfers through one shaped port and one
unshaped control, and judges them under the tolerance its docstring states:

```sh
sudo -n /usr/local/libexec/calque-netshape up 500 20 45650
python3 scripts/netshape/probe.py calibrate 500 20 45650
sudo -n /usr/local/libexec/calque-netshape down
```

Record `uptime` beside each result.
