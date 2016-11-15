# WeblogChallenge
This is an interview challenge for Paytm Labs. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

WARNING: Running WeblongChallenge.main() will result in ~1.5GB of data created

##Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

    As per https://en.wikipedia.org/wiki/Session_(web_analytics)#Time-oriented_approaches I have Sessionized the web log with a 15 minute window.

    The Session objects are serialized as json to data/goal1.json/

2. Determine the average session time

    The average session length was calculated to be ~100.73 seconds long

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

    The (Session, Int) tuples are serialized as json to data/goal3.json/

4. Find the most engaged users, ie the IPs with the longest session times

    The (Long, Session) tuples are serialized as json to data/goal4.json/