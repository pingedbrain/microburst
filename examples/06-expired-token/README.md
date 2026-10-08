# Expired token mid-session

Credentials expire — STS tokens, SSO sessions, IRSA rotations lagging.
The app has been running fine for an hour, then every call starts
failing with `ExpiredTokenException`. Does your credential refresh kick
in, or does the error bubble up as a service outage?

```bash
microburst -c examples/06-expired-token/chaos.yml -u http://localhost:4566
python examples/06-expired-token/app.py
```

`times: 3` makes exactly the first 3 calls fail — enough to see the
failure and the recovery in one run. In a real deployment you'd point
this at every service, or at `sts`/`sso` alone to simulate the provider
side failing.
