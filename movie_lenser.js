// ************************************************************************ //
// Average of all average ratings
//   This is to determine the scale of the rating (is there rating bias?)
// ************************************************************************ //
function all_user_avg_ratings() {
    sum = 0.0;
    num_users = 0;
    c = db.users.find();
    while ( c.hasNext() ) {
        user = c.next();
        // printjson(user["avg_rating"]);
        sum += user["avg_rating"];
        num_users += 1;
    }
    print(sum/num_users);
}
